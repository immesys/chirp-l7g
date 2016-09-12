package chirpl7g

import (
	"encoding/binary"
	"fmt"

	"gopkg.in/immesys/bw2bind.v5"
)

type L7GHeader struct {
	Srcmac  string
	Srcip   string
	Popid   string
	Poptime int64
	Brtime  int64
	Rssi    int64
	Lqi     int64
	Payload []byte
}
type ChirpHeader struct {
	Type     int
	Build    int
	Seqno    uint16
	CalPulse uint16
	CalRes   []uint16
	Uptime   uint64
	Primary  bool
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
		po.ValueInto(&h)
		if h.Payload[0] > 20 {
			fmt.Println("Skipping packet", h.Payload[0])
			continue
		}
		ch := ChirpHeader{}
		LoadChirpHeader(h.Payload, &ch)
	}
}
func LoadChirpHeader(arr []byte, h *ChirpHeader) {
	h.Type = int(arr[0])
	h.Build = binary.LittleEndian.Uint16(arr[1:])
	h.Seqno = binary.LittleEndian.Uint16(arr[3:])
	h.CalPulse = binary.LittleEndian.Uint16(arr[5:])
	h.CalRes = make([]uint16, 4)
	for i := 0; i < 4; i++ {
		h.CalRes[i] = binary.LittleEndian.Uint16(arr[7+2*i])
	}
	//15
	h.Uptime = binary.LittleEndian.Uint64(arr[15:])
	h.Primary = arr[23] != 0
	h.Data = make([][]byte, 4)
	for i := 0; i < 4; i++ {
		h.Data[i] = arr[24+70*i : 24+70*(i+1)]
	}
}
