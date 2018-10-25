package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/abhisheknsit/boomer/boomer"
	"github.com/tcnksm/go-httpstat"
	"github.com/newrelic/go-agent"
)

var (
	httpClient          *http.Client
	maxIdleConnections  int
	testDefinitionsFile string
	postData            []byte
	keepAlive           bool
	dataArraySize       int64
	throughPutWait      int
	b                   []byte
)

const (
	RequestTimeout int = 0
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

type weightParams struct {
	Magnitude int
	Frequency int
	Constant  int
	Phase     int
}

type header struct {
	name  string
	value int64
}

type Test struct {
	Url     string       `json:"url,omitempty"`
	Headers []header     `json:"header,omitempty"`
	Body    int64        `json:"body,omitempty"`
	Weight  weightParams `json:"weight,omitempty"`
	Method  string       `json:"method,omitempty"`
	Wait    int16        `json:"wait,omitempty"`
}

type suite struct {
	suite []Test
}

func RandStringBytes(n int64) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return string(b)
}

func createHTTPClient() *http.Client {
	log.Println("KEEP_ALIVE", keepAlive)

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxIdleConnections,
			MaxIdleConns:        maxIdleConnections,
			DisableKeepAlives:   keepAlive,
		},
		Timeout: time.Duration(RequestTimeout) * time.Second,
	}

	return client
}

func httpReq(method string, url string, bodysize int64, headers []header, wait1 int16) func() {
	//file := postData[:bodysize]
	if dataArraySize == 0 {
		log.Println("DataArraySize was 0")
		dataArraySize, _ = strconv.ParseInt(os.Getenv("DATA_ARRAY_SIZE"), 10, 0)
		throughPutWait, _ = strconv.Atoi(os.Getenv("THROUGH_PUT_WAIT"))

	}
	return func() {
		var req *http.Request
		var elapsed int64
		var wait int64
		pr, pw := io.Pipe()
		go func() {
			for i := int64(0); i < bodysize/dataArraySize; i++ {
				pw.Write(postData)
				time.Sleep(time.Duration(throughPutWait) * time.Millisecond)
			}
			if bodysize%dataArraySize != 0 {
				pw.Write(postData[:(bodysize % dataArraySize)])
			}
			pw.Close()
		}()
		start := boomer.Now()
		req, _ = http.NewRequest(method, url, pr)
		if method != "GET" {
			req.Close = true
		}
		if headers != nil {
			for _, header := range headers {
				req.Header.Set(header.Name, RandStringBytes(header.Value))
				log.Println("Setting header: ", header.Name)
			}
		}
		//Using http stat and handing over the request to the context
		var result httpstat.Result
		ctxt := httpstat.WithHTTPStat(req.Context(), &result)
		req = req.WithContext(ctxt)

		resp, err := httpClient.Do(req)
		result.End(time.Now())
		elapsed = boomer.Now() - start
		if elapsed < 0 {
			elapsed = 0
		}
		reqIns := boomer.RequestInsights {
			int64(result.DNSLookup / time.Millisecond),
			int64(result.TCPConnection / time.Millisecond),
			int64(result.TLSHandshake / time.Millisecond),
			int64(result.ServerProcessing / time.Millisecond),
			int64(result.NameLookup / time.Millisecond),
			int64(result.Connect / time.Millisecond),
			int64(result.Pretransfer / time.Millisecond),
			int64(result.StartTransfer / time.Millisecond),
			elapsed,
		}
		if err != nil {
			log.Println(err)
			elapsed = boomer.Now() - start
			if elapsed < 0 {
				elapsed = 0
			}
			boomer.Events.Publish("request_failure", method, url, elapsed, err.Error())
		} else {
			defer resp.Body.Close()
			for {
				_, err = resp.Body.Read(b)
				if err != nil {
					break
				}
			}

			_, _ = ioutil.ReadAll(resp.Body)
			elapsed = boomer.Now() - start
			if elapsed < 0 {
				elapsed = 0
			}
			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				boomer.Events.Publish("request_failure", method, url, elapsed, strconv.Itoa(resp.StatusCode))
			} else {
				boomer.Events.Publish("request_success", method, url, reqIns, resp.ContentLength)
			}
		}
		if elapsed < 1000 {
			wait = 1000 - elapsed
		} else {
			wait = 0
		}
		time.Sleep(time.Duration(wait) * time.Millisecond)
	}
}

func WeightFn(params weightParams) func() int {
	return func() (weight int) {
		base := 0.0
		if params.Frequency != 0 {
			base = math.Cos(float64(time.Now().Unix())*(2*math.Pi/float64(params.Frequency)) + float64(params.Phase))
		}
		weight = int(base*float64(params.Magnitude)) + params.Constant
		if weight < 0 {
			weight = 0
		}
		return
	}
}

func getTaskParams(testDefinition Test) *boomer.Task {
	fn := httpReq(testDefinition.Method, testDefinition.Url, testDefinition.Body, testDefinition.Headers, testDefinition.Wait)
	weightFn := WeightFn(testDefinition.Weight)
	task := &boomer.Task{
		Name:     testDefinition.Url,
		WeightFn: weightFn,
		Fn:       fn,
	}
	//taskJson, _ := json.Marshal(task)
	log.Println(testDefinition.Method, testDefinition.Url, testDefinition.Body, testDefinition.Wait)
	return task
}

func main() {
	config := newrelic.NewConfig(os.Getenv("NEW_RELIC_APPNAME"), os.Getenv("NEW_RELIC_LICENSE"))
	_, _ = newrelic.NewApplication(config)

	log.Println("Executing main function")
	rawTestDefinitions, _ := ioutil.ReadFile(testDefinitionsFile)
	log.Println("FileContent", string(rawTestDefinitions))
	var testDefinitions []Test
	err := json.Unmarshal(rawTestDefinitions, &testDefinitions)
	if err != nil {
		log.Println(err.Error())
	}
	var taskList []*boomer.Task

	for i, testDefinition := range testDefinitions {
		log.Println(i)
		log.Println(testDefinition.Method, testDefinition.Url, testDefinition.Body)
		taskList = append(taskList, getTaskParams(testDefinition))
	}

	// Shuffle taskList
	for i := len(taskList) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		taskList[i], taskList[j] = taskList[j], taskList[i]
	}

	boomer.Run(taskList...)
}

func init() {
	maxIdleConnections, _ = strconv.Atoi(os.Getenv("MAX_IDLE_CONNECTIONS"))
	log.Println("MaxIdleConnections", maxIdleConnections)
	testDefinitionsFile = os.Getenv("TEST_DEFINITIONS")
	var err error
	dataArraySize, err = strconv.ParseInt(os.Getenv("DATA_ARRAY_SIZE"), 10, 0)
	if err != nil {
		log.Println("Error Setting DataArray Size", err.Error())
		dataArraySize = 10000000
	}
	log.Println("DataArray Size", dataArraySize)
	throughPutWait, err = strconv.Atoi(os.Getenv("THROUGH_PUT_WAIT"))
	if err != nil {
		log.Println("Error Setting throughPutWait", err.Error())
		throughPutWait = 0
	}
	log.Println("ThroughputWait:", throughPutWait)
	log.Println("TestDefinition File", testDefinitionsFile)
	if ka, err := strconv.ParseBool(os.Getenv("KEEP_ALIVE")); err == nil {
		keepAlive = !ka
	} else {
		keepAlive = true
	}
	log.Println("KEEP_ALIVE", keepAlive)
	postData = make([]byte, dataArraySize)
	b = make([]byte, dataArraySize)
	httpClient = createHTTPClient()
}
