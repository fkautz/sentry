package aprs

import (
	"fmt"
)

// PacketType represents the type of information contains in an APRS packet.
type PacketType byte

var packetTypeNames = map[byte]string{
	0x1c: "Current Mic-E Data (Rev 0 beta)",
	0x1d: "Old Mic-E Data (Rev 0 beta)",
	'!':  "Position without timestamp (no APRS messaging), or Ultimeter 2000 WX Station",
	'#':  "Peet Bros U-II Weather Station",
	'$':  "Raw GPS data or Ultimeter 2000",
	'%':  "Agrelo DFJr / MicroFinder",
	'"':  "Old Mic-E Data (but Current data for TM-D700)",
	')':  "Item",
	'*':  "Peet Bros U-II Weather Station",
	',':  "Invalid data or test data",
	'/':  "Position with timestamp (no APRS messaging)",
	':':  "Message",
	';':  "Object",
	'<':  "Station Capabilities",
	'=':  "Position without timestamp (with APRS messaging)",
	'>':  "Status",
	'?':  "Query",
	'@':  "Position with timestamp (with APRS messaging)",
	'T':  "Telemetry data",
	'[':  "Maidenhead grid locator beacon (obsolete)",
	'_':  "Weather Report (without position)",
	'`':  "Current Mic-E Data (not used in TM-D700)",
	'{':  "User-Defined APRS packet format",
	'}':  "Third-party traffic",
}

// IsMessage is true if this PacketType represents a message.
func (p PacketType) IsMessage() bool {
	return p == ':'
}

// IsThirdParty is true if this PacketType is sent via third party.
func (p PacketType) IsThirdParty() bool {
	return p == '}'
}

func (p PacketType) String() string {
	if t, ok := packetTypeNames[byte(p)]; ok {
		return t
	}
	return fmt.Sprintf("Unknown %x", byte(p))
}
