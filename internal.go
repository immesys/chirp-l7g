package chirpl7g

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/immesys/ragent/ragentlib"

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
}

func (a *dataProcessingAlgorithm) MirrorToStandardOutput(v bool) {
	a.EmitToStdout = v
}
func runDPA(entitycontents []byte, iz func(e Emitter), cb func(info *SetInfo, popHdr []*L7GHeader, h []*ChirpHeader, e Emitter), vendor string, algorithm string) error {
	infoc := color.New(color.FgBlue, color.Bold)
	errc := color.New(color.FgRed, color.Bold)
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

	a := dataProcessingAlgorithm{}
	cl, err := bw2bind.Connect("127.0.0.1:28588")
	if err != nil {
		return err
	}
	a.BWCL = cl
	a.Process = cb
	a.Initialize = iz
	a.Vendor = vendor
	a.LastRawInput = make(map[string]RawInputData)
	a.Algorithm = algorithm
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

	a.Initialize(&a)
	a.Uncorrectable = make(map[string]int)
	a.Total = make(map[string]int)

	procCH := make(chan *bw2bind.SimpleMessage, 1000)
	go func() {
		for m := range ch {
			select {
			case procCH <- m:
			default:
				fmt.Printf("dropping message\n")
			}
		}
	}()
	a.handleIncomingData(procCH)
	fmt.Fprintf(os.Stderr, "HIC returned\n")
	return errors.New("could not consume data fast enough")
}

func (a *dataProcessingAlgorithm) handleIncomingData(in chan *bw2bind.SimpleMessage) {

	batchInfo := make(map[string]*SetInfo)
	batchL7G := make(map[string][]*L7GHeader)
	batchChirp := make(map[string][]*ChirpHeader)
	lastseq := make(map[string]int)

	for m := range in {
		//m.Dump()
		parts := strings.Split(m.URI, "/")
		site := parts[2]

		po := m.GetOnePODF(bw2bind.PODFL7G1Raw).(bw2bind.MsgPackPayloadObject)
		h := L7GHeader{}
		po.ValueInto(&h)
		if h.Payload[0] != 9 {
			fmt.Printf("Skipping l7g packet type %d\n", h.Payload[0])
			continue
		}

		// if h.Payload[1] > 20 {
		// 	//Skip the xor packets for now
		// 	fmt.Println("Skipping xor packet", h.Payload[0])
		// 	continue
		// }

		ch := ChirpHeader{}

		isAnemometer := loadChirpHeader(h.Payload, &ch)
		if !isAnemometer {
			fmt.Printf("is not anemometer\n")
			continue
		}

		did := h.Srcmac
		numasics := 4
		if ch.Build%10 == 7 {
			numasics = 6
		}
		lastseqi, ok := lastseq[did]
		if !ok {
			lastseqi = int(ch.Seqno - 10)
		}
		uncorrectablei, ok := a.Uncorrectable[h.Srcmac]
		if !ok {
			uncorrectablei = 0
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
			l7g := batchL7G[did]
			hdr := batchChirp[did]
			info := batchInfo[did]
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
			batchL7G[did] = make([]*L7GHeader, numasics)
			batchChirp[did] = make([]*ChirpHeader, numasics)
			batchInfo[did] = &SetInfo{
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
		if int(ch.Seqno) != lastseqi {
			//TODO this is not valid for 6 channel
			uncorrectablei++
		}
		lastseqi = int(ch.Seqno)
		lastseq[h.Srcmac] = lastseqi
		a.Uncorrectable[h.Srcmac] = uncorrectablei

		//Save the total packets
		totali, ok := a.Total[h.Srcmac]
		if !ok {
			totali = 0
		}
		totali++
		a.Total[h.Srcmac] = totali

		//Save this header
		batchL7G[did][ch.Primary] = &h
		batchChirp[did][ch.Primary] = &ch

		//Check for recovered packets
		df
	}
}
func (a *dataProcessingAlgorithm) Data(od OutputData) {
	od.Vendor = a.Vendor
	od.Algorithm = a.Algorithm

	od.Uncorrectable, _ = a.Uncorrectable[od.Sensor]
	od.Total, _ = a.Total[od.Sensor]
	od.Correctable, _ = a.Correctable[od.Sensor]
	od.RawInput = a.LastRawInput[od.Sensor]
	//spew.Dump(a.LastRawInput)
	//spew.Dump(od)
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
var xormap_actual map[int][]byte
var xormap_parity map[int][]byte
var xormu sync.Mutex

func init() {
	xormap_actual = make(map[int][]byte)
	xormap_parity = make(map[int][]byte)
}
func check_xor(seqno int, arr []byte) {
	xormu.Lock()
	defer xormu.Unlock()
	cmp := make([]byte, len(arr))
	for i := 0; i < 4; i++ {
		thisbuf, ok := xormap_actual[seqno-i]
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
	fmt.Printf("XOR RESULT: %x\n", cmp)
}
func insert_xor(seqno int, arr []byte) {
	xormu.Lock()
	defer xormu.Unlock()
	xormap_parity[seqno] = arr
}
func w(seqno int) int {
	return seqno & 0xFFFF
}
func recurse_xor(seqno int) {
	xormu.Lock()
	defer xormu.Unlock()
	parity, ok := xormap_parity[seqno]
	if !ok {
		return
	}
	cmp := make([]byte, 76)
	copy(cmp[:], parity[:])
	var missingseqno int
	var nmissing int
	for i := 0; i < 4; i++ {
		thisbuf, ok := xormap_actual[w(seqno-i)]
		if !ok {
			nmissing++
			missingseqno = w(seqno - i)
			continue
		}
		for x := 0; x < len(cmp); x++ {
			cmp[x] ^= thisbuf[x]
		}
	}
	if nmissing == 0 {
		return
	}
	if nmissing > 1 {
		return
	}
	fmt.Printf("recovered a packet\n")
	recovered := cmp
	recovered[0] = 9
	recovered[1] = 1
	recovered[2] = byte(missingseqno & 0xff)
	recovered[3] = byte(missingseqno >> 8)
	xormap_actual[missingseqno] = recovered
}
func insertChirpHeader(arr []byte) int {
	if arr[0] != 9 {
		return
	}

	Type := int(arr[1])
	seqno := binary.LittleEndian.Uint16(arr[2:])

	if Type > 15 {
		insert_xor(int(seqno), arr)
		//check_xor(int(h.Seqno), arr)
		recurse_xor(int(seqno))
		return -1
	}
	var paritycheck uint8 = 0
	for _, b := range arr {
		paritycheck ^= b
	}
	if paritycheck != 0 {
		return -1
	}
	xormu.Lock()
	xormap_actual[int(seqno)] = arr
	xormu.Unlock()
	return seqno
}
func loadChirpHeader(seqno int, h *ChirpHeader) bool {

	xormu.Lock()
	arr := xormal_actual[seqno]
	xormu.Unlock()
	//Drop the type info we added
	if arr[0] != 9 {
		return false
	}

	h.Type = int(arr[1])
	h.Seqno = binary.LittleEndian.Uint16(arr[2:])

	if h.Type > 15 {
		insert_xor(int(h.Seqno), arr)
		//check_xor(int(h.Seqno), arr)
		extraarr := recurse_xor(int(h.Seqno))
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
	xormu.Lock()
	xormap_actual[int(h.Seqno)] = arr
	xormu.Unlock()
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
	//fmt.Printf("arr: %x\n", arr)

	for i := 0; i < numasics; i++ {
		if h.Build%10 == 5 || h.Build%10 == 0 {
			if i == int(h.Primary) {
				h.MaxIndex[i] = -1
				continue
			}
		}
		if h.Build%10 == 7 {
			if int(h.Primary) < 3 && i < 3 {
				h.MaxIndex[i] = -1
				continue
			}
			if int(h.Primary) >= 3 && i >= 3 {
				h.MaxIndex[i] = -1
				continue
			}
		}
		h.MaxIndex[i] = int(arr[22+offset])
		h.IValues[i] = make([]int, 4)
		h.QValues[i] = make([]int, 4)
		for j := 0; j < 4; j++ {
			h.QValues[i][j] = int(int16(binary.LittleEndian.Uint16(arr[28+offset*16+j*4:])))
			h.IValues[i][j] = int(int16(binary.LittleEndian.Uint16(arr[28+offset*16+j*4+2:])))
		}
		offset++
	}

	f_acc_x := int16(binary.LittleEndian.Uint16(arr[6:]))
	f_acc_y := int16(binary.LittleEndian.Uint16(arr[8:]))
	f_acc_z := int16(binary.LittleEndian.Uint16(arr[10:]))
	f_mag_x := int16(binary.LittleEndian.Uint16(arr[12:]))
	f_mag_y := int16(binary.LittleEndian.Uint16(arr[14:]))
	f_mag_z := int16(binary.LittleEndian.Uint16(arr[16:]))
	//f_hdc_tmp := int16(binary.LittleEndian.Uint16(arr[18:]))
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
	//h.Humidity = float64(f_hdc_rh) / 100.0
	//h.Temperature = float64(f_hdc_tmp) / 100.0
	//fmt.Printf("Received frame:\n")
	//spew.Dump(h)
	return true
}
