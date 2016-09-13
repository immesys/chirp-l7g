package main

import (
	"encoding/binary"
	"fmt"
	"math"

	"gopkg.in/immesys/bw2bind.v5"
)

const AlgVer = "ref_1_0__passthrough"
const Vendor = "UCBerkeley"

type L7GHeader struct {
	Srcmac  string `msgpack:"srcmac"`
	Srcip   string `msgpack:"srcip"`
	Popid   string `msgpack:"popid"`
	Poptime int64  `msgpack:"poptime"`
	Brtime  int64  `msgpack:"brtime"`
	Rssi    int    `msgpack:"rssi"`
	Lqi     int    `msgpack:"lqi"`
	Payload []byte `msgpack:"payload"`
}
type ChirpHeader struct {
	Type     int
	Build    int
	Seqno    uint16
	CalPulse uint16
	CalRes   []uint16
	Uptime   uint64
	Primary  uint8
	Data     [][]byte
}

func main() {
	cl := bw2bind.ConnectOrExit("")
	cl.SetEntityFromEnvironOrExit()
	ch := cl.SubscribeOrExit(&bw2bind.SubscribeParams{
		URI:       "ucberkeley/sasc/+/s.hamilton/+/i.l7g/signal/raw",
		AutoChain: true,
	})
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
		LoadChirpHeader(h.Payload, &ch)
		od := Deliver(&h, &ch)
		if od != nil {
			PublishOData(cl, od)
		}
	}
}
func PublishOData(cl *bw2bind.BW2Client, od *Odata) {
	URI := fmt.Sprintf("ucberkeley/anemometer/data/%s/s.anemometer/%s/i.anemometerdata/signal/feed", od.Vendor, od.Algorithm)
	fmt.Println("uri is: ", URI)
	po, err := bw2bind.CreateMsgPackPayloadObject(bw2bind.PONumChirpFeed, od)
	if err != nil {
		panic(err)
	}
	err = cl.Publish(&bw2bind.PublishParams{
		URI:            URI,
		AutoChain:      true,
		PayloadObjects: []bw2bind.PayloadObject{po},
	})
	if err != nil {
		fmt.Println("Got publish error: ", err)
	}
}
func LoadChirpHeader(arr []byte, h *ChirpHeader) {
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
func Deliver(popHdr *L7GHeader, h *ChirpHeader) *Odata {
	magic_count_tx := -4
	odata := Odata{
		Timestamp: popHdr.Brtime,
		Sensor:    popHdr.Srcmac,
		Vendor:    Vendor,
		Algorithm: AlgVer,
	}
	for set := 0; set < 4; set++ {
		if int(h.Primary) == set {
			continue
		}
		//There are actually four sets of data, one from each chip
		data := h.Data[set]

		//The first six bytes of the data
		tof_sf := binary.LittleEndian.Uint16(data[0:2])
		tof_est := binary.LittleEndian.Uint16(data[2:4])
		intensity := binary.LittleEndian.Uint16(data[4:6])

		//Load the complex numbers
		iz := make([]int16, 16)
		qz := make([]int16, 16)
		for i := 0; i < 16; i++ {
			qz[i] = int16(binary.LittleEndian.Uint16(data[6+4*i:]))
			iz[i] = int16(binary.LittleEndian.Uint16(data[6+4*i+2:]))
		}

		//Find the largest complex magnitude (as a square)
		magsqr := make([]uint64, 16)
		magmax := uint64(0)
		for i := 0; i < 16; i++ {
			magsqr[i] = uint64(int64(qz[i])*int64(qz[i]) + int64(iz[i])*int64(iz[i]))
			if magsqr[i] > magmax {
				magmax = magsqr[i]
			}
		}

		//Find the first index to be greater than half the max (quarter the square)
		quarter := magmax / 4
		less_idx := 0
		greater_idx := 0
		for i := 0; i < 16; i++ {
			if magsqr[i] < quarter {
				less_idx = i
			}
			if magsqr[i] > quarter {
				greater_idx = i
				break
			}
		}
		if greater_idx == 0 {
			odata.Extradata = append(odata.Extradata, "Scale error!")
			fmt.Println("[DATA ERROR] scale is wrong")
			continue
		}

		less_val := math.Sqrt(float64(magsqr[less_idx]))
		greater_val := math.Sqrt(float64(magsqr[greater_idx]))
		half_val := math.Sqrt(float64(quarter))
		//CalPulse is in microseconds
		freq := float64(tof_sf) / 2048 * float64(h.CalRes[set]) / (float64(h.CalPulse) / 1000)
		lerp_idx := float64(less_idx) + (half_val-less_val)/(greater_val-less_val)
		tof := (lerp_idx + float64(magic_count_tx)) / freq * 8
		fmt.Printf("SEQ %d ASIC %d primary=%d\n", h.Seqno, set, h.Primary)
		fmt.Println("lerp_idx: ", lerp_idx)
		fmt.Println("tof_sf: ", tof_sf)
		fmt.Println("freq: ", freq)
		fmt.Printf("tof: %.2f us\n", tof*1000000)
		fmt.Println("intensity: ", intensity)
		fmt.Println("tof chip estimate: ", tof_est)
		fmt.Println("tof 50us estimate: ", lerp_idx*50)
		fmt.Println("data: ")
		for i := 0; i < 16; i++ {
			fmt.Printf(" [%2d] %6d + %6di (%.2f)\n", i, qz[i], iz[i], math.Sqrt(float64(magsqr[i])))
		}
		fmt.Println(".")

		odata.Tofs = append(odata.Tofs, TOFMeasure{
			Src: int(h.Primary),
			Dst: set,
			Val: tof * 1000000,
		})
	}
	return &odata
}

type TOFMeasure struct {
	Src int     `msgpack:"src"`
	Dst int     `msgpack:"dst"`
	Val float64 `msgpack:"val"`
}

type Odata struct {
	Timestamp int64        `msgpack:"time"`
	Sensor    string       `msgpack:"sensor"`
	Vendor    string       `msgpack:"vendor"`
	Algorithm string       `msgpack:"algorithm"`
	Tofs      []TOFMeasure `msgpack:"tofs"`
	Extradata []string     `msgpack:"extradata"`
}
