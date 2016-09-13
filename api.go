package chirpl7g

import (
	"encoding/binary"
	"fmt"

	"gopkg.in/immesys/bw2bind.v5"
)

type dataProcessingAlgorithm struct {
	BWCL       *bw2bind.BW2Client
	Vendor     string
	Algorithm  string
	Process    func(popHdr *L7GHeader, h *ChirpHeader, e Emitter)
	Initialize func(e Emitter)
}

// Encapsulates information added by the layer 7 gateway point of presence
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

// Encapsulates the raw information transmitted by the anemometer.
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
// new data arrives. This function does not return
func RunDPA(iz func(e Emitter), cb func(popHdr *L7GHeader, h *ChirpHeader, e Emitter)) {
	a := dataProcessingAlgorithm{}
	cl := bw2bind.ConnectOrExit("")
	a.BWCL = cl
	a.Process = cb
	a.Initialize = iz
	cl.SetEntityFromEnvironOrExit()
	ch := cl.SubscribeOrExit(&bw2bind.SubscribeParams{
		URI:       "ucberkeley/sasc/+/s.hamilton/+/i.l7g/signal/raw",
		AutoChain: true,
	})
	a.Initialize(&a)
	for m := range ch {
		po := m.GetOnePODF(bw2bind.PODFL7G1Raw).(bw2bind.MsgPackPayloadObject)
		h := L7GHeader{}
		//	fmt.Println(po.TextRepresentation())
		po.ValueInto(&h)
		//	fmt.Printf("%#v, po: %#v\n", h, po)
		if h.Payload[0] > 20 {
			fmt.Println("Skipping packet", h.Payload[0])
			continue
		}
		ch := ChirpHeader{}
		loadChirpHeader(h.Payload, &ch)
		a.Process(&h, &ch, &a)
	}
}
func (a *dataProcessingAlgorithm) Data(od OutputData) {
	od.Vendor = a.Vendor
	od.Algorithm = a.Algorithm
	URI := fmt.Sprintf("ucberkeley/anemometer/data/%s/%s/s.anemometer/%s/i.anemometerdata/signal/feed", od.Vendor, od.Algorithm, od.Sensor)
	fmt.Println("uri is: ", URI)
	po, err := bw2bind.CreateMsgPackPayloadObject(bw2bind.PONumChirpFeed, od)
	if err != nil {
		panic(err)
	}
	err = a.BWCL.Publish(&bw2bind.PublishParams{
		URI:            URI,
		AutoChain:      true,
		PayloadObjects: []bw2bind.PayloadObject{po},
	})
	if err != nil {
		fmt.Println("Got publish error: ", err)
	}
}
func loadChirpHeader(arr []byte, h *ChirpHeader) {
	h.Type = int(arr[0])
	h.Seqno = binary.LittleEndian.Uint16(arr[1:])
	h.Build = int(binary.LittleEndian.Uint16(arr[3:]))
	h.CalPulse = binary.LittleEndian.Uint16(arr[5:])
	h.CalRes = make([]uint16, 4)
	for i := 0; i < 4; i++ {
		h.CalRes[i] = binary.LittleEndian.Uint16(arr[7+2*i:])
	}
	//15
	h.Uptime = binary.LittleEndian.Uint64(arr[15:])
	h.Primary = arr[23]
	h.Data = make([][]byte, 4)
	for i := 0; i < 4; i++ {
		h.Data[i] = arr[24+70*i : 24+70*(i+1)]
	}
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

// OutputData encapsulates a single set of measurements taken at roughly the same
// time
type OutputData struct {
	// The time, in nanoseconds since the epoch, that this set of measurements was taken
	Timestamp int64 `msgpack:"time"`
	// The symbol name of the sensor (like a variable name, no spaces etc)
	Sensor string `msgpack:"sensor"`
	// The name of the vendor (you) that wrote the data processing algorithm, also variable characters only
	Vendor string `msgpack:"vendor"`
	// The symbol name of the algorithm, including version, parameters. also variable characters only
	Algorithm string `msgpack:"algorithm"`
	// The set of time of flights in this output data set
	Tofs []TOFMeasure `msgpack:"tofs"`
	// Any extra string messages (like X is malfunctioning), these are displayed in the log on the UI
	Extradata []string `msgpack:"extradata"`
}

// An emitter is used to report OutputData that you have generated
type Emitter interface {
	// Emit an output data set
	Data(OutputData)
}
