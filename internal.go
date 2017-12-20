package chirpl7g

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/fatih/color"
	"github.com/immesys/ragent/ragentlib"

	"gopkg.in/immesys/bw2bind.v5"
)

const serverVK = "MT3dKUYB8cnIfsbnPrrgy8Cb_8whVKM-Gtg2qd79Xco="
const serverIP = "52.9.16.254:28590"

type dataProcessingAlgorithm struct {
	BWCL          *bw2bind.BW2Client
	Vendor        string
	Algorithm     string
	Process       func(popHdr *L7GHeader, h *ChirpHeader, e Emitter)
	Initialize    func(e Emitter)
	Uncorrectable map[string]int
	Total         map[string]int
	Correctable   map[string]int
}

func runDPA(entitycontents []byte, iz func(e Emitter), cb func(popHdr *L7GHeader, h *ChirpHeader, e Emitter), vendor string, algorithm string) error {
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
	a.Algorithm = algorithm
	_, err = cl.SetEntity(entitycontents)
	if err != nil {
		return err
	}

	infoc.Printf("tapping hamilton feeds\n")
	ch, err := cl.Subscribe(&bw2bind.SubscribeParams{
		URI:       fmt.Sprintf("ucberkeley/anem/+/+/s.hamilton/+/i.l7g/signal/raw"),
		AutoChain: true,
	})
	if err == nil {
		infoc.Printf("tap complete\n")
	} else {
		errc.Printf("tap failed: %v\n", errc)
		os.Exit(1)
	}

	a.Initialize(&a)
	lastseq := make(map[string]int)
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

	for m := range procCH {
		po := m.GetOnePODF(bw2bind.PODFL7G1Raw).(bw2bind.MsgPackPayloadObject)
		h := L7GHeader{}
		po.ValueInto(&h)
		if h.Payload[0] != 9 {
			//fmt.Printf("Skipping l7g packet type %d\n", h.Payload[0])
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
			continue
		}
		lastseqi, ok := lastseq[h.Srcmac]
		if !ok {
			lastseqi = int(ch.Seqno - 1)
		}
		uncorrectablei, ok := a.Uncorrectable[h.Srcmac]
		if !ok {
			uncorrectablei = 0
		}
		lastseqi++
		lastseqi &= 0xFFFF
		if int(ch.Seqno) != lastseqi {
			uncorrectablei++
			lastseqi = int(ch.Seqno)
		}
		lastseq[h.Srcmac] = lastseqi
		a.Uncorrectable[h.Srcmac] = uncorrectablei
		totali, ok := a.Total[h.Srcmac]
		if !ok {
			totali = 0
		}
		totali++
		a.Total[h.Srcmac] = totali

		a.Process(&h, &ch, &a)
	}
	return errors.New("could not consume data fast enough")
}
func (a *dataProcessingAlgorithm) Data(od OutputData) {
	od.Vendor = a.Vendor
	od.Algorithm = a.Algorithm
	URI := fmt.Sprintf("ucberkeley/anemometer/data/%s/%s/s.anemometer/%s/i.anemometerdata/signal/feed", od.Vendor, od.Algorithm, od.Sensor)
	od.Uncorrectable, _ = a.Uncorrectable[od.Sensor]
	od.Total, _ = a.Total[od.Sensor]
	od.Correctable, _ = a.Correctable[od.Sensor]
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
		fmt.Println("Got publish error: ", err)
	} else {
		//fmt.Println("Publish ok")
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
func loadChirpHeader(arr []byte, h *ChirpHeader) bool {
	//Drop the type info we added
	if arr[0] != 9 {
		fmt.Printf("in load chirp, type is %d\n", arr[0])
		return false
	}
	h.Type = int(arr[1])
	h.Seqno = binary.LittleEndian.Uint16(arr[2:])

	if h.Type > 15 {
		check_xor(int(h.Seqno), arr)
		return false
	}

	var paritycheck uint8 = 0

	if paritycheck != 0 {
		fmt.Printf("Received packet failed parity check, ignoring %d\n", paritycheck)
		expected_seqnos := arr[2] ^ (arr[2] - 1) ^ (arr[2] - 2) ^ (arr[2] - 3)
		fmt.Printf("B: %d\n", paritycheck^arr[2])
		fmt.Printf("C: %d\n", expected_seqnos)
		return false
	}
	xormap[int(h.Seqno)] = arr
	h.Build = int(arr[5])
	h.CalPulse = 160
	h.Primary = arr[4]
	h.CalRes = make([]int, 4)
	for i := 0; i < 4; i++ {
		if i == (int(h.Primary)) {
			h.CalRes[i] = int(binary.LittleEndian.Uint16(arr[26:]))
		} else {
			h.CalRes[i] = -1
		}
	}

	h.MaxIndex = make([]int, 4)
	h.IValues = make([][]int, 4)
	h.QValues = make([][]int, 4)
	offset := 0
	for i := 0; i < 4; i++ {
		if i == int(h.Primary) {
			h.MaxIndex[i] = -1
			continue
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
	f_hdc_tmp := int16(binary.LittleEndian.Uint16(arr[18:]))
	f_hdc_rh := binary.LittleEndian.Uint16(arr[20:])

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
	h.Humidity = float64(f_hdc_rh) / 100.0
	h.Temperature = float64(f_hdc_tmp) / 100.0
	//fmt.Printf("Received frame:\n")
	spew.Dump(h)
	return true
}
