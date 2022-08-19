package orc

import (
	"bytes"
	"encoding/hex"
	"io/ioutil"
	"testing"

	gproto "github.com/golang/protobuf/proto"
	"github.com/scritchley/orc/proto"
)

func TestZlibDecode(t *testing.T) {
	bs, _ := hex.DecodeString("0002009212e46016982926f176e97d0e8556468d3973e59532387884d81999985958d9d8a5980b32f3a438d373cbe27312935273a4f8e1ccf89cfc744303296e90405a6a62496951aa942012072acf99939f9e99179f9c5722c591545a199f925859acd0c0a4a166c0a5c4c9c18e60b291c064c1641acc992b6fc5c63167ae7c008355208856f2e45230303030303449354b4db4344d334c3332363336b330b5484b4d4d32493135361352484b4b4b4b3131304e4eb1484b4b4e33313330b248364fb64831493536364e35906838778e3f80016e342e5a10440b717230085448ac39b0822380c14a082cc4c5c120708a55e2d39b2bac010c0e13fc3c9801010000ffff")
	reader := bytes.NewReader(bs)

	compression := &CompressionZlib{}
	dec := compression.Decoder(reader)
	footerBytes, _ := ioutil.ReadAll(dec)

	footer := new(proto.Footer)
	err := gproto.Unmarshal(footerBytes, footer)
	if err != nil {
		t.Error(err)
	}

	t.Log(footer.String())

}
