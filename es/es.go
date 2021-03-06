package es

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ESResponse struct {
	Status   int         `json:"status"`
	Error    interface{} `json:"error"`
	ScrollID string      `json:"_scroll_id"`
	Hits     HitsWrapper `json:"hits"`
}

//type ESError struct {
//	Type   string `json:"type"`
//	Reason string `json:"reason"`
//}

type HitsWrapper struct {
	Total interface{} `json:"total"`
	Hits  []Hit       `json:"hits"`
}

type Hit struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

type QueryWrapper struct {
	Query Query `json:"query"`
	Size  int64 `json:"size"`
}

type Query struct {
	Bool Bool `json:"bool"`
}

type Bool struct {
	Must []interface{} `json:"must"`
}

type TermCondition struct {
	Term map[string]string `json:"term"`
}

type WildcardCondition struct {
	Wildcard map[string]string `json:"wildcard"`
}

type RegexpCondition struct {
	Regexp map[string]string `json:"regexp"`
}

type AllCondition struct {
	All interface{} `json:"match_all"`
}

var DefaultHttpClient *http.Client

func Init(timeout int64, proxyUrl string) {
	DefaultHttpClient = newHttpClient(timeout, proxyUrl)
}

func newHttpClient(timeout int64, proxyUrl string) *http.Client {
	proxy := func(_ *http.Request) (*url.URL, error) {
		if proxyUrl == "" {
			return nil, nil
		}
		return url.Parse(proxyUrl)
	}
	transport := &http.Transport{Proxy: proxy}
	return &http.Client{Transport: transport, Timeout: time.Duration(timeout) * time.Millisecond}
}

func WalkEs(esURL, indexName string, batchSize int64, minQueryInterval time.Duration,
	termFilter map[string]string, wildcardFilter map[string]string, regexpFilter map[string]string,
	action func(hits []Hit)) (int64, error) {
	if esURL == "" || indexName == "" || batchSize <= 0 {
		return 0, errors.New("invalid parameters")
	}
	count := int64(0)
	scrollId, audioHit, err := firstPassES(esURL+"/"+indexName, batchSize, termFilter, wildcardFilter, regexpFilter)
	for {
		if err != nil {
			return 0, err
		}
		if len(audioHit) == 0 {
			break
		}
		if scrollId == "" {
			return 0, errors.New("empty scrollId")
		}
		action(audioHit)
		count += int64(len(audioHit))
		log.Println("count:", count)
		<-time.After(minQueryInterval)
		scrollId, audioHit, err = passES(esURL, scrollId)
	}
	return count, nil
}

func firstPassES(indexURL string, batchSize int64,
	termFilter, wildcardFilter, regexpFilter map[string]string) (string, []Hit, error) {

	mustItems := make([]interface{}, 0, 1)

	if termFilter != nil && len(termFilter) > 0 {
		for k, v := range termFilter {
			term := TermCondition{Term: map[string]string{k: v}}
			mustItems = append(mustItems, term)
		}
	}

	if wildcardFilter != nil && len(wildcardFilter) > 0 {
		for k, v := range wildcardFilter {
			term := WildcardCondition{Wildcard: map[string]string{k: v}}
			mustItems = append(mustItems, term)
		}
	}

	if regexpFilter != nil && len(regexpFilter) > 0 {
		for k, v := range regexpFilter {
			term := RegexpCondition{Regexp: map[string]string{k: v}}
			mustItems = append(mustItems, term)
		}
	}

	queryWrapper := &QueryWrapper{Query: Query{Bool: Bool{Must: mustItems}}, Size: batchSize}
	jsonstr, err := json.Marshal(queryWrapper)
	if err != nil {
		return "", nil, err
	}

	log.Println("query str:" + string(jsonstr))
	req, err := http.NewRequest("POST", indexURL+"/_search?scroll=1m", bytes.NewReader(jsonstr))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Close = true
	res, err := DefaultHttpClient.Do(req)
	if err != nil {
		log.Println("post error, post body:", string(jsonstr), "error:", err.Error())
		return "", nil, err
	}
	defer res.Body.Close()
	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", nil, err
	}

	var esRes ESResponse
	err = json.Unmarshal(content, &esRes)
	if err != nil {
		return "", nil, err
	}
	if esRes.Status != 0 {
		errMsg := ""
		switch esRes.Error.(type) {
		case string:
			errMsg = esRes.Error.(string)
		default:
			b, _ := json.Marshal(esRes.Error)
			errMsg = string(b)
		}
		return "", nil, errors.New("es query exception, exp :" + errMsg)
	}
	if esRes.ScrollID == "" {
		return "", nil, errors.New("empty scrollID for first request, reqBody:" + string(jsonstr) + ", res:" + string(content))
	}

	total := 0
	switch esRes.Hits.Total.(type) {
	case float64:
		total = int(esRes.Hits.Total.(float64))
	case map[string]interface{}:
		totalStruc := esRes.Hits.Total.(map[string]interface{})
		total = int(totalStruc["value"].(float64))
	}
	if total == 0 {
		return "", make([]Hit, 0), nil
	}

	return esRes.ScrollID, esRes.Hits.Hits, nil
}

func passES(esURL, scrollId string) (string, []Hit, error) {

	reqBody := "{\"scroll\":\"1m\", \"scroll_id\":\"" + scrollId + "\"}"

	req, err := http.NewRequest("POST", esURL+"/_search/scroll", strings.NewReader(reqBody))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Close = true
	res, err := DefaultHttpClient.Do(req)
	if err != nil {
		log.Println("post error, post body:", reqBody)
		return "", nil, err
	}
	defer res.Body.Close()
	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", nil, err
	}

	var esRes ESResponse
	err = json.Unmarshal(content, &esRes)
	if err != nil {
		return "", nil, err
	}
	if esRes.Status != 0 {
		errMsg := ""
		switch esRes.Error.(type) {
		case string:
			errMsg = esRes.Error.(string)
		default:
			b, _ := json.Marshal(esRes.Error)
			errMsg = string(b)
		}
		return "", nil, errors.New("es query exception, exp:" + errMsg)
	}
	if esRes.ScrollID == "" {
		return "", nil, errors.New("empty scrollID, reqBody:" + reqBody)
	}
	total := 0
	switch esRes.Hits.Total.(type) {
	case float64:
		total = int(esRes.Hits.Total.(float64))
	case map[string]interface{}:
		totalStruc := esRes.Hits.Total.(map[string]interface{})
		total = int(totalStruc["value"].(float64))
	}
	if total == 0 {
		return "", make([]Hit, 0), nil
	}
	return esRes.ScrollID, esRes.Hits.Hits, nil
}

func queryES(indexURL string, batchSize int64, termFilter map[string]string, wildcardFilter map[string]string) (int64, []Hit, error) {

	mustItems := make([]interface{}, 0, 1)

	if termFilter != nil && len(termFilter) > 0 {
		for k, v := range termFilter {
			term := TermCondition{Term: map[string]string{k: v}}
			mustItems = append(mustItems, term)
		}
	}

	if wildcardFilter != nil && len(wildcardFilter) > 0 {
		for k, v := range wildcardFilter {
			term := WildcardCondition{Wildcard: map[string]string{k: v}}
			mustItems = append(mustItems, term)
		}
	}

	queryWrapper := &QueryWrapper{Query: Query{Bool: Bool{Must: mustItems}}, Size: batchSize}
	jsonstr, err := json.Marshal(queryWrapper)
	if err != nil {
		return 0, nil, err
	}

	req, err := http.NewRequest("POST", indexURL+"/_search", bytes.NewReader(jsonstr))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Close = true

	res, err := DefaultHttpClient.Do(req)
	if err != nil {
		log.Println("post error, post body:", string(jsonstr))
		return 0, nil, err
	}
	defer res.Body.Close()
	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return 0, nil, err
	}

	var esRes ESResponse
	err = json.Unmarshal(content, &esRes)
	if err != nil {
		return 0, nil, err
	}
	if esRes.Status != 0 {
		errMsg := ""
		switch esRes.Error.(type) {
		case string:
			errMsg = esRes.Error.(string)
		default:
			b, _ := json.Marshal(esRes.Error)
			errMsg = string(b)
		}
		return 0, nil, errors.New("es query exception, exp:" + errMsg)
	}
	total := 0
	switch esRes.Hits.Total.(type) {
	case float64:
		total = int(esRes.Hits.Total.(float64))
	case map[string]interface{}:
		totalStruc := esRes.Hits.Total.(map[string]interface{})
		total = int(totalStruc["value"].(float64))
	}
	if total == 0 {
		return 0, make([]Hit, 0), nil
	}

	return int64(total), esRes.Hits.Hits, nil
}
