package chirpl7g

import (
	"encoding/binary"
	"fmt"

	"gopkg.in/immesys/bw2bind.v5"
)

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

// L7GHeader encapsulates information added by the layer 7 gateway point of presence
type L7GHeader struct {
	// The MAC (8 bytes) of the sending anemometer, hex encoded
	Srcmac string `msgpack:"srcmac"`
	// The source ipv6 address of the sending anemometer, may not be globally routed
	Srcip string `msgpack:"srcip"`
	// The identifier of the point of presence that detected the packet. Used for duplicate detection
	Popid string `msgpack:"popid"`
	// The time on the point of presence (in us), may not be absolute wall time
	Poptime int64 `msgpack:"poptime"`
	// The time on the border router when the message was transmitted on bosswave, nanoseconds since the epoch
	Brtime int64 `msgpack:"brtime"`
	// The RSSI of the received packet, used for ranging
	Rssi int `msgpack:"rssi"`
	// The link quality indicator of the received packet, may not always be useful
	Lqi int `msgpack:"lqi"`
	// The raw payload of the packet, you should not need this as this is decoded into the ChirpHeader structure for you
	Payload []byte `msgpack:"payload"`
}

// ChirpHeader encapsulates the raw information transmitted by the anemometer.
type ChirpHeader struct {
	// This field can be ignored, it is used by the forward error correction layer
	Type int
	// This field is incremented for every measurement transmitted. It can be used for detecting missing packets
	Seqno uint16
	// This is the build number (firmware version) programmed on the sensor
	Build int
	// This is the length of the calibration pulse (in ms)
	CalPulse uint16
	// This is array of ticks that each asic measured the calibration pulse as
	CalRes []uint16
	// This is how long the anemometer has been running, in microseconds
	Uptime uint64
	// This is the number of the ASIC that was in OPMODE_TXRX, the rest were in OPMODE_RX
	Primary uint8
	// The four 70 byte arrays of raw data from the ASICs
	Data [][]byte
}

// RunDPA will execute a data processing algorithm. Pass it a function that will be invoked whenever
// new data arrives. You must pass it an initializer function, an on-data funchion and then
// your name (the vendor) and the name of the algorithm. This function does not return
func RunDPA(iz func(e Emitter), cb func(popHdr *L7GHeader, h *ChirpHeader, e Emitter), vendor string, algorithm string) {
	a := dataProcessingAlgorithm{}
	cl := bw2bind.ConnectOrExit("")
	a.BWCL = cl
	a.Process = cb
	a.Initialize = iz
	a.Vendor = vendor
	a.Algorithm = algorithm
	cl.SetEntityFromEnvironOrExit()
	ch := cl.SubscribeOrExit(&bw2bind.SubscribeParams{
		URI:       "ucberkeley/sasc/+/s.hamilton/+/i.l7g/signal/raw",
		AutoChain: true,
	})
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
		if h.Payload[2] > 20 {
			//Skip the xor packets for now
			//fmt.Println("Skipping packet", h.Payload[0])
			continue
		}

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
33 typedef struct __attribute__((packed))
34 {
35   uint16_t l7type;
36   uint8_t type;
37   uint16_t seqno;
38   uint16_t build;
39   uint16_t cal_pulse;
40   uint16_t calres[4];
41   uint64_t uptime;
42   uint8_t primary;
43   uint8_t data[4][70];
44 } measure_set_t;
*/
func loadChirpHeader(arr []byte, h *ChirpHeader) bool {
	//Drop the type info we added
	ht := binary.LittleEndian.Uint16(arr)
	if ht != 7 {
		return false
	}
	h.Type = int(arr[2])
	h.Seqno = binary.LittleEndian.Uint16(arr[3:])
	h.Build = int(binary.LittleEndian.Uint16(arr[5:]))
	h.CalPulse = binary.LittleEndian.Uint16(arr[7:])
	h.CalRes = make([]uint16, 4)
	for i := 0; i < 4; i++ {
		h.CalRes[i] = binary.LittleEndian.Uint16(arr[9+2*i:])
	}
	//17
	h.Uptime = binary.LittleEndian.Uint64(arr[17:])
	h.Primary = arr[25]
	h.Data = make([][]byte, 4)
	for i := 0; i < 4; i++ {
		h.Data[i] = arr[26+70*i : 26+70*(i+1)]
	}
	return true
}

// TOFMeasure is a single time of flight measurement. The time of the measurement
// is ingerited from the OutputData that contains it
type TOFMeasure struct {
	// SRC is the index [0,4) of the ASIC that emitted the chirp
	Src int `msgpack:"src"`
	// DST is the index [0,4) of the ASIC that the TOF was read from
	Dst int `msgpack:"dst"`
	// Val is the time of flight, in microseconds
	Val float64 `msgpack:"val"`
}

type VelocityMeasure struct {
	//Velocity in m/s
	X float64 `msgpack:"x"`
	Y float64 `msgpack:"y"`
	//Z is the vertical dimension
	Z float64 `msgpack:"z"`
}

// OutputData encapsulates a single set of measurements taken at roughly the same
// time
type OutputData struct {
	// The time, in nanoseconds since the epoch, that this set of measurements was taken
	Timestamp int64 `msgpack:"time"`
	// The symbol name of the sensor (like a variable name, no spaces etc)
	Sensor string `msgpack:"sensor"`
	// The name of the vendor (you) that wrote the data processing algorithm, also variable characters only
	// This gets set to the value passed to RunDPA automatically
	Vendor string `msgpack:"vendor"`
	// The symbol name of the algorithm, including version, parameters. also variable characters only
	// This gets set to the value passed to RunDPA automatically
	Algorithm string `msgpack:"algorithm"`
	// The set of time of flights in this output data set
	Tofs []TOFMeasure `msgpack:"tofs"`
	// The set of velocities in this output data set
	Velocities []VelocityMeasure `msgpack:"velocities"`
	// Any extra string messages (like X is malfunctioning), these are displayed in the log on the UI
	Extradata     []string `msgpack:"extradata"`
	Uncorrectable int      `msgpack:"uncorrectable"`
	Correctable   int      `msgpack:"correctable"`
	Total         int      `msgpack:"total"`
}

// Emitter is used to report OutputData that you have generated
type Emitter interface {
	// Emit an output data set
	Data(OutputData)
}
