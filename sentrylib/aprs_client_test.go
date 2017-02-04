package sentrylib

//
//import (
//	"testing"
//	"github.com/docker/docker/pkg/testutil/assert"
//	"log"
//	"io/ioutil"
//	"github.com/spf13/viper"
//	"bytes"
//)
//
//func TestNewAprsClient(t *testing.T) {
//
//	file, err := ioutil.ReadFile("../.sentry.yaml")
//	assert.NilError(t, err)
//
//	viper.SetConfigType("yaml")
//	err = viper.ReadConfig(bytes.NewBuffer(file))
//	assert.NilError(t, err)
//
//	config := Config{}
//	viper.Unmarshal(&config)
//
//	client := NewAprsClient(config.AprsServer, config.AprsUser, config.AprsPasscode, config.AprsFilter)
//	err = client.Dial()
//	defer client.Close()
//	assert.NilError(t, err)
//
//	for i := 0; i < 10; i++ {
//		if client.Next() {
//			frame, err := client.Frame()
//			assert.NilError(t, err)
//			assert.NilError(t, client.Error())
//			log.Println(frame)
//		} else {
//			assert.NilError(t, client.Error())
//			break
//		}
//	}
//}
//
//func BenchmarkNewAprsClient(b *testing.B) {
//	b.StopTimer()
//	file, err := ioutil.ReadFile("../.sentry.yaml")
//	assert.NilError(b, err)
//
//	viper.SetConfigType("yaml")
//	err = viper.ReadConfig(bytes.NewBuffer(file))
//	assert.NilError(b, err)
//
//	config := Config{}
//	viper.Unmarshal(&config)
//
//	client := NewAprsClient(config.AprsServer, config.AprsUser, config.AprsPasscode, config.AprsFilter)
//	err = client.Dial()
//	defer client.Close()
//	assert.NilError(b, err)
//
//	valid := true
//	b.ResetTimer()
//	b.StartTimer()
//	for i := 0; i < b.N; i++ {
//		if client.Next() {
//			frame, err := client.Frame()
//			assert.NilError(b, err)
//			assert.NilError(b, client.Error())
//			log.Println(frame)
//		} else {
//			assert.NilError(b, client.Error())
//			break
//		}
//	}
//	b.StopTimer()
//	log.Println(valid)
//}
