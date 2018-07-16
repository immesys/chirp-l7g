package chirpl7g

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/immesys/ragent/ragentlib"
	"github.com/jacobsa/go-serial/serial"

	"gopkg.in/immesys/bw2bind.v5"
)

const serverVK = "MT3dKUYB8cnIfsbnPrrgy8Cb_8whVKM-Gtg2qd79Xco="
const serverIP = "wave-ragent-974638466.us-west-1.elb.amazonaws.com:28590"

type dataProcessingAlgorithm struct {
	BWCL          *bw2bind.BW2Client
	Vendor        string
	Algorithm     string
	Process       func(info *SetInfo, popHdr []*L7GHeader, h []*ChirpHeader, e Emitter)
	Initialize    func(e Emitter)
	Uncorrectable map[string]int
	Total         map[string]int
	Correctable   map[string]int
	LastRawInput  map[string]RawInputData
	EmitToStdout  bool

	batchInfo  map[string]*SetInfo
	batchL7G   map[string][]*L7GHeader
	batchChirp map[string][]*ChirpHeader
	lastseq    map[string]int
}

type localMessage struct {
	Packet []byte
	IQ     []byte
}

func (a *dataProcessingAlgorithm) MirrorToStandardOutput(v bool) {
	a.EmitToStdout = v
}
func runDPA(entitycontents []byte, iz func(e Emitter), cb func(info *SetInfo, popHdr []*L7GHeader, h []*ChirpHeader, e Emitter), vendor string, algorithm string, local string) error {
	infoc := color.New(color.FgBlue, color.Bold)
	errc := color.New(color.FgRed, color.Bold)

	var cl *bw2bind.BW2Client
	if local == "" {

		var err error
		go func() {
			defer func() {
				r := recover()
				if r != nil {
					errc.Printf("failed to connect ragent: %v", r)
					os.Exit(1)
				}
			}()
			ragentlib.DoClientER([]byte(entitycontents), serverIP, serverVK, "127.0.0.1:28588")
		}()
		time.Sleep(200 * time.Millisecond)
		cl, err = bw2bind.Connect("127.0.0.1:28588")
		if err != nil {
			return err
		}
	}
	a := dataProcessingAlgorithm{}
	a.BWCL = cl
	a.Process = cb
	a.Initialize = iz
	a.Vendor = vendor
	a.LastRawInput = make(map[string]RawInputData)
	a.Algorithm = algorithm
	a.Initialize(&a)
	a.Uncorrectable = make(map[string]int)
	a.Total = make(map[string]int)
	procCH := make(chan *bw2bind.SimpleMessage, 1000)

	if local == "" {
		vk, err := cl.SetEntity(entitycontents[1:])
		if err != nil {
			return err
		}
		fmt.Printf("our VK is %v\n", vk)
		infoc.Printf("tapping hamilton feeds\n")
		sitestring := "+"
		if os.Getenv("SITE_FILTER") != "" {
			sitestring = os.Getenv("SITE_FILTER")
		}
		ch, err := cl.Subscribe(&bw2bind.SubscribeParams{
			URI:       fmt.Sprintf("ucberkeley/anem/%s/+/s.hamilton/+/i.l7g/signal/raw", sitestring),
			AutoChain: true,
		})
		if err == nil {
			infoc.Printf("tap complete\n")
		} else {
			errc.Printf("tap failed: %v\n", errc)
			os.Exit(1)
		}

		go func() {
			for m := range ch {
				select {
				case procCH <- m:
				default:
					fmt.Printf("dropping message\n")
				}
			}
		}()

	}

	lchan := a.openLocal(local)
	a.handleIncomingData(procCH, lchan)
	fmt.Fprintf(os.Stderr, "HIC returned\n")
	return errors.New("could not consume data fast enough")
}

func (a *dataProcessingAlgorithm) openLocal(local string) chan *localMessage {
	if local == "" {
		return nil
	}

	options := serial.OpenOptions{
		PortName:        local,
		BaudRate:        115200,
		DataBits:        8,
		StopBits:        1,
		MinimumReadSize: 4,
	}

	// Open the port.
	port, err := serial.Open(options)
	if err != nil {
		log.Fatalf("serial.Open: %v", err)
	}

	lchan := make(chan *localMessage, 100)
	br := bufio.NewReader(port)
	go a.serialProcessingLoop(br, lchan)
	return lchan

}

func (a *dataProcessingAlgorithm) serialProcessingLoop(br *bufio.Reader, lchan chan *localMessage) {
	for {
		marker := "cafebabe"
		buf := make([]byte, 82+64*3)
	outer:
		for {
			for i := 0; i < len(marker); i++ {
				c, err := br.ReadByte()
				if err != nil {
					panic(err)
				}
				if c != marker[i] {
					continue outer
				}
			}
			break
		}

		//read payload
		_, err := io.ReadFull(br, buf)
		if err != nil {
			panic(err)
		}
		lchan <- &localMessage{
			Packet: buf[:82],
			IQ:     buf[82:],
		}
	}
}

func (a *dataProcessingAlgorithm) procPayload(site string, h L7GHeader, iq []byte) {
	ch := ChirpHeader{}
	isAnemometer := loadChirpHeader(h.Payload, &ch, iq)
	if !isAnemometer {
		return
	}

	did := h.Srcmac
	numasics := 4
	if ch.Build%10 == 7 {
		numasics = 6
	}
	lastseqi, ok := a.lastseq[did]
	if !ok {
		lastseqi = int(ch.Seqno - 10)
	}
	lastseqi &= 0xFFFF
	lastsegment := lastseqi / 4
	currentsegment := ch.Seqno / 4
	if ch.Build%10 == 7 {
		lastsegment = lastseqi / 8
		currentsegment = ch.Seqno / 8
	}

	if int(currentsegment) != int(lastsegment) {
		//Send the last segment if it is not nil
		l7g := a.batchL7G[did]
		hdr := a.batchChirp[did]
		info := a.batchInfo[did]
		if !(info == nil || hdr == nil || l7g == nil) {
			//Maybe we have to send
			mustsend := false
			for i := 0; i < numasics; i++ {
				if hdr[i] != nil {
					mustsend = true
				}
			}
			if mustsend {
				complete := true
				var t time.Time
				hast := false
				for i := 0; i < numasics; i++ {
					if l7g[i] == nil {
						complete = false
					} else if !hast {
						t = time.Unix(0, l7g[i].Brtime)
						hast = true
					}
				}
				info.Complete = complete
				info.TimeOfFirst = t
				ri := RawInputData{
					SetInfo:      info,
					L7GHeaders:   l7g,
					ChirpHeaders: hdr,
				}
				a.LastRawInput[did] = ri
				a.Process(info, l7g, hdr, a)
			}
		}
		a.batchL7G[did] = make([]*L7GHeader, numasics)
		a.batchChirp[did] = make([]*ChirpHeader, numasics)
		a.batchInfo[did] = &SetInfo{
			Site:    site,
			MAC:     did,
			Build:   ch.Build,
			IsDuct:  ch.Build%10 == 5,
			IsRoom:  ch.Build%10 == 0,
			IsDuct6: ch.Build%10 == 7,
		}
	}

	//Save the sequence number
	lastseqi++
	lastseqi &= 0xFFFF

	lastseqi = int(ch.Seqno)
	a.lastseq[h.Srcmac] = lastseqi

	//Save the total packets
	totali, ok := a.Total[h.Srcmac]
	if !ok {
		totali = 0
	}
	totali++
	a.Total[h.Srcmac] = totali

	//Save this header
	a.batchL7G[did][ch.Primary] = &h
	a.batchChirp[did][ch.Primary] = &ch
}
func (a *dataProcessingAlgorithm) handleIncomingData(in chan *bw2bind.SimpleMessage, lm chan *localMessage) {

	a.batchInfo = make(map[string]*SetInfo)
	a.batchL7G = make(map[string][]*L7GHeader)
	a.batchChirp = make(map[string][]*ChirpHeader)
	a.lastseq = make(map[string]int)

	go func() {
		for m := range in {
			//m.Dump()
			parts := strings.Split(m.URI, "/")
			site := parts[2]

			po := m.GetOnePODF(bw2bind.PODFL7G1Raw).(bw2bind.MsgPackPayloadObject)
			h := L7GHeader{}
			po.ValueInto(&h)
			if h.Payload[0] != 9 {
				//fmt.Printf("Skipping l7g packet type %d\n", h.Payload[0])
				continue
			}

			a.procPayload(site, h, nil)

		}
	}()
	for m := range lm {
		h := L7GHeader{}
		h.Brtime = time.Now().UnixNano()
		h.Srcmac = "usblocal"
		h.Payload = m.Packet
		a.procPayload("usblocal", h, m.IQ)
	}
}
func (a *dataProcessingAlgorithm) Data(od OutputData) {
	od.Vendor = a.Vendor
	od.Algorithm = a.Algorithm

	od.Total, _ = a.Total[od.Sensor]
	od.RawInput = a.LastRawInput[od.Sensor]
	//spew.Dump(a.LastRawInput)
	//spew.Dump(od)
	const dopublish = false
	if dopublish {
		URI := fmt.Sprintf("ucberkeley/anem/%s/%s/%s/s.anemometer/%s/i.anemometerdata/signal/feed", od.Vendor, od.Algorithm, od.RawInput.SetInfo.Site, od.Sensor)
		po, err := bw2bind.CreateMsgPackPayloadObject(bw2bind.PONumChirpFeed, od)
		if err != nil {
			panic(err)
		}
		doPersist := false
		if od.Total%200 < 5 {
			doPersist = true
		}
		err = a.BWCL.Publish(&bw2bind.PublishParams{
			URI:            URI,
			AutoChain:      true,
			PayloadObjects: []bw2bind.PayloadObject{po},
			Persist:        doPersist,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Got publish error: %v\n", err)
		} else {
			//fmt.Println("Publish ok")
		}
	}
	if a.EmitToStdout {
		data, err := json.Marshal(od)
		if err != nil {
			panic(err)
		}
		fmt.Println("MIRROR_STDOUT:" + string(data))
	}
}

/*
typedef struct __attribute__((packed))
{
  uint8_t   l7type;     // 0
  uint8_t   type;       // 1
  uint16_t  seqno;      // 2:3
  uint8_t   primary;    // 4
  uint8_t   buildnum;   // 5
  int16_t   acc_x;      // 6:7
  int16_t   acc_y;      // 8:9
  int16_t   acc_z;      // 10:11
  int16_t   mag_x;      // 12:13
  int16_t   mag_y;      // 14:15
  int16_t   mag_z;      // 16:17
  int16_t   hdc_temp;   // 18:19
  int16_t   hdc_hum;    // 20:21
  uint8_t   max_index[3]; // 22:24
  uint8_t   parity;    // 25
  uint16_t  cal_res;   // 26:27
  //Packed IQ data for 4 pairs
  //M-3, M-2, M-1, M
  uint8_t data[3][16];  //28:75
} measure_set_t; //76 bytes
*/
var xormap map[int][]byte

func init() {
	xormap = make(map[int][]byte)
}
func check_xor(seqno int, arr []byte) {
	cmp := make([]byte, len(arr))
	for i := 0; i < 4; i++ {
		thisbuf, ok := xormap[seqno-i]
		if !ok {
			return
		}
		for x := 0; x < len(arr); x++ {
			cmp[x] ^= thisbuf[x]
		}
	}
	for x := 0; x < len(arr); x++ {
		cmp[x] ^= arr[x]
	}
	//fmt.Printf("XOR RESULT: %x\n", cmp)
}
func loadChirpHeader(arr []byte, h *ChirpHeader, iqarray []byte) bool {
	//Drop the type info we added
	if arr[0] != 9 {
		return false
	}

	h.Type = int(arr[1])
	h.Seqno = binary.LittleEndian.Uint16(arr[2:])

	if h.Type > 15 {
		//check_xor(int(h.Seqno), arr)
		return false
	}

	var paritycheck uint8 = 0
	for _, b := range arr {
		paritycheck ^= b
	}
	if paritycheck != 0 {
		fmt.Fprintf(os.Stderr, "Received packet failed parity check, ignoring %d\n", paritycheck)
		//expected_seqnos := arr[2] ^ (arr[2] - 1) ^ (arr[2] - 2) ^ (arr[2] - 3)
		//fmt.Printf("B: %d\n", paritycheck^arr[2])
		//fmt.Printf("C: %d\n", expected_seqnos)
		return false
	}
	xormap[int(h.Seqno)] = arr
	h.Build = int(arr[5])
	numasics := 4
	if h.Build%10 == 7 {
		numasics = 6
	}
	h.CalPulse = 160
	h.Primary = arr[4]
	h.CalRes = make([]int, numasics)
	for i := 0; i < numasics; i++ {
		if i == (int(h.Primary)) {
			h.CalRes[i] = int(binary.LittleEndian.Uint16(arr[26:]))
		} else {
			h.CalRes[i] = -1
		}
	}

	h.MaxIndex = make([]int, numasics)
	h.IValues = make([][]int, numasics)
	h.QValues = make([][]int, numasics)
	offset := 0
	dotofs := false
	if iqarray != nil {
		h.FullIValues = make([][]int, numasics)
		h.FullQValues = make([][]int, numasics)
	}
	if len(arr) > 78 {
		h.TOFSF = make([]int, numasics)
		dotofs = true
	}
	for i := 0; i < numasics; i++ {
		if h.Build%10 == 5 || h.Build%10 == 0 {
			if i == int(h.Primary) {
				h.MaxIndex[i] = -1
				if dotofs {
					h.TOFSF[i] = -1
				}
				continue
			}
		}
		if h.Build%10 == 7 {
			if int(h.Primary) < 3 && i < 3 {
				h.MaxIndex[i] = -1
				if dotofs {
					h.TOFSF[i] = -1
				}
				continue
			}
			if int(h.Primary) >= 3 && i >= 3 {
				h.MaxIndex[i] = -1
				if dotofs {
					h.TOFSF[i] = -1
				}
				continue
			}
		}
		h.MaxIndex[i] = int(arr[22+offset])
		//fmt.Printf("Max Index [%d-%d] %d\n", h.Primary, i, h.MaxIndex[i])
		h.IValues[i] = make([]int, 4)
		h.QValues[i] = make([]int, 4)
		if iqarray != nil {
			h.FullIValues[i] = make([]int, 16)
			h.FullQValues[i] = make([]int, 16)
		}
		for j := 0; j < 4; j++ {
			h.QValues[i][j] = int(int16(binary.LittleEndian.Uint16(arr[28+offset*16+j*4:])))
			h.IValues[i][j] = int(int16(binary.LittleEndian.Uint16(arr[28+offset*16+j*4+2:])))
		}
		if iqarray != nil {
			for j := 0; j < 16; j++ {
				h.FullQValues[i][j] = int(int16(binary.LittleEndian.Uint16(iqarray[offset*64+j*4:])))
				h.FullIValues[i][j] = int(int16(binary.LittleEndian.Uint16(iqarray[offset*64+j*4+2:])))
			}
		}
		if dotofs {
			iv := int(uint16(binary.LittleEndian.Uint16(arr[76+offset*2:])))
			h.TOFSF[i] = iv
		}
		offset++
	}

	f_acc_x := int16(binary.LittleEndian.Uint16(arr[6:]))
	f_acc_y := int16(binary.LittleEndian.Uint16(arr[8:]))
	f_acc_z := int16(binary.LittleEndian.Uint16(arr[10:]))
	f_mag_x := int16(binary.LittleEndian.Uint16(arr[12:]))
	f_mag_y := int16(binary.LittleEndian.Uint16(arr[14:]))
	f_mag_z := int16(binary.LittleEndian.Uint16(arr[16:]))
	f_tmp := int16(binary.LittleEndian.Uint16(arr[18:]))
	//f_hdc_rh := binary.LittleEndian.Uint16(arr[20:])

	if f_acc_x >= 8192 {
		f_acc_x -= 16384
	}
	if f_acc_y >= 8192 {
		f_acc_y -= 16384
	}
	if f_acc_z >= 8192 {
		f_acc_z -= 16384
	}
	h.Accelerometer = []float64{
		float64(f_acc_x) * 0.244,
		float64(f_acc_y) * 0.244,
		float64(f_acc_z) * 0.244,
	}
	h.Magnetometer = []float64{
		float64(f_mag_x) * 0.1,
		float64(f_mag_y) * 0.1,
		float64(f_mag_z) * 0.1,
	}
	if f_tmp == 0 {
		h.Temperature = nil
	} else {
		t := (float64(f_tmp)/65536)*175.72 - 46.85
		h.Temperature = &t
	}

	return true
}
