package chirpl7g

// L7GHeader encapsulates information added by the layer 7 gateway point of presence
type L7GHeader struct {
	// The site that this data is from
	Site string `msgpack:"-"`
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
	// duct anemometers end in 5 and room anemometers end in 0
	Build int
	// This is the length of the calibration pulse (in ms)
	CalPulse int
	// This is array of ticks that each asic measured the calibration pulse as
	// Only the Primary field is filled in, the other fields will contain -1
	CalRes []int
	// This is the number of the ASIC that was in OPMODE_TXRX, the rest were in OPMODE_RX
	Primary uint8
	// The IQ indexes where the maximum occurs
	// the Primary will have -1
	MaxIndex []int

	// The IQ values leading up to and including the maximum. There will always
	// be four values, so if the maximum is <=3 we will start at 0 and include
	// some points after the maximum. The primary index will have nil slices
	IValues [][]int
	QValues [][]int

	// The accelerometer values, X,Y,Z in milli G
	Accelerometer []float64
	// The magnetometer values X,Y,Z in micro tesla
	Magnetometer []float64

	// Air temperature as measured by the Hamilton in Celsius
	Temperature float64
	// Air relative humidity as measured by the hamilton in percent
	Humidity float64
}

// RunDPA will execute a data processing algorithm. Pass it a function that will be invoked whenever
// new data arrives. You must pass it an initializer function, an on-data funchion and then
// your name (the vendor) and the name of the algorithm. This function does not return
func RunDPA(entitycontents []byte, iz func(e Emitter), cb func(popHdr *L7GHeader, h *ChirpHeader, e Emitter), vendor string, algorithm string) error {
	return runDPA(entitycontents, iz, cb, vendor, algorithm)
}

// TOFMeasure is a single time of flight measurement. The time of the measurement
// is inherited from the OutputData that contains it
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
	// Positive X should be due north in cases where that is known
	X float64 `msgpack:"x"`
	// Positive Y should be due east in cases where that is known
	Y float64 `msgpack:"y"`
	// Positive Z should be up
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
	Extradata []string `msgpack:"extradata"`

	// Information about the signal quality to the anemometer, this gets filled in automatically
	Uncorrectable int `msgpack:"uncorrectable"`
	Correctable   int `msgpack:"correctable"`
	Total         int `msgpack:"total"`
}

// Emitter is used to report OutputData that you have generated
type Emitter interface {
	// Emit an output data set
	Data(OutputData)
}
