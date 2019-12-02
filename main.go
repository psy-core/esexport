package main

import (
    "flag"
    "fmt"
    "github.com/psy-core/esexport/es"
    "log"
    "os"
    "strconv"
    "strings"
    "time"
)

var (
    ES_URL                  = flag.String("e", "http://172.22.97.11:9200", "es url")
    INDEX_NAME              = flag.String("i", "", "索引名称")
    COLUMNS                 = flag.String("c", "sid", "提取的字段，逗号分割")
    BATCH_SIZE              = flag.Int64("b", 100, "batch size")
    QUERY_INTERVAL          = flag.Int64("interval", 0, "query interval in milliseconds")
    FILTER_TERM_COLUMNS     = flag.String("tf", "logtype:demolog", "term过滤字段，格式为key:value,key:value...")
    FILTER_WILDCARD_COLUMNS = flag.String("wf", "logtype:demolog*", "wildcard过滤字段，格式为key:value,key:value...")
    FILTER_REGEXP_COLUMNS   = flag.String("rf", "logtype:(demolog1)|(demolog2)", "regex过滤字段，格式为key:value,key:value...")
    PROXY_URL               = flag.String("p", "", "代理，比如 socks5://127.0.0.1:1800")
    TIMEOUT                 = flag.Int64("t", 10000, "每次post请求超时时间，毫秒")
    OUTFILE                 = flag.String("o", "output.txt", "输出文件")
)

func main() {
    flag.Parse()
    log.Println("============ ES  CONFIG =====================")
    log.Println("ES_URL", *ES_URL)
    log.Println("INDEX_NAME", *INDEX_NAME)
    log.Println("COLUMNS", *COLUMNS)
    log.Println("BATCH_SIZE", *BATCH_SIZE)
    log.Println("QUERY_INTERVAL", *QUERY_INTERVAL)
    log.Println("FILTER_TERM_COLUMNS", *FILTER_TERM_COLUMNS)
    log.Println("FILTER_WILDCARD_COLUMNS", *FILTER_WILDCARD_COLUMNS)
    log.Println("FILTER_REGEXP_COLUMNS", *FILTER_REGEXP_COLUMNS)
    log.Println("PROXY_URL", *PROXY_URL)
    log.Println("TIMEOUT", *TIMEOUT)
    log.Println("OUTFILE", *OUTFILE)
    log.Println("=============================================")

    if *INDEX_NAME == "" {
        log.Println("index name must not be empty")
        os.Exit(-1)
    }

    es.Init(*TIMEOUT, *PROXY_URL)
    fd, err := os.OpenFile(*OUTFILE, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
    if err != nil {
        log.Println("open file ", *OUTFILE, "error:", err.Error())
        os.Exit(-1)
    }
    defer func() {
        err = fd.Close()
        if err != nil {
            log.Println("close file ", *OUTFILE, "error:", err.Error())
            os.Exit(-1)
        }
    }()

    columns := strings.Split(*COLUMNS, ",")
    termFilters := make(map[string]string)
    for _, line := range strings.Split(*FILTER_TERM_COLUMNS, ",") {
        if line == "" {
            continue
        }
        token := strings.Split(line, ":")
        if len(token) == 2 {
            termFilters[strings.TrimSpace(token[0])] = strings.TrimSpace(token[1])
        }
    }
    wildcardFilters := make(map[string]string)
    for _, line := range strings.Split(*FILTER_WILDCARD_COLUMNS, ",") {
        if line == "" {
            continue
        }
        token := strings.Split(line, ":")
        if len(token) == 2 {
            wildcardFilters[strings.TrimSpace(token[0])] = strings.TrimSpace(token[1])
        }
    }
    regexpFilters := make(map[string]string)
    for _, line := range strings.Split(*FILTER_REGEXP_COLUMNS, ",") {
        if line == "" {
            continue
        }
        token := strings.Split(line, ":")
        if len(token) == 2 {
            regexpFilters[strings.TrimSpace(token[0])] = strings.TrimSpace(token[1])
        }
    }

    action := func(hits []es.Hit) {
        for _, hit := range hits {
            for _, c := range columns {
                if c == "" {
                    continue
                }
                var value string
                switch hit.Source[c].(type) {
                case float64:
                    value = strconv.FormatFloat(hit.Source[c].(float64), 'f', 0, 64)
                case float32:
                    value = strconv.FormatFloat(float64(hit.Source[c].(float32)), 'f', 0, 32)
                default:
                    value = fmt.Sprintf("%v", hit.Source[c])
                }
                if value == "" {
                    value = "<nil>"
                }
                _, err = fd.WriteString(fmt.Sprintf("%v\t", value))
                if err != nil {
                    log.Println("write string to file ", *OUTFILE, "error:", err.Error())
                }
            }
            _, err = fd.WriteString("\n")
            if err != nil {
                log.Println("write newline to file ", *OUTFILE, "error:", err.Error())
            }
        }
    }

    count, err := es.WalkEs(*ES_URL, *INDEX_NAME, *BATCH_SIZE, time.Duration(*QUERY_INTERVAL)*time.Millisecond,
        termFilters, wildcardFilters, regexpFilters, action)
    if err != nil {
        log.Println("walk es error:", err.Error())
        os.Exit(-1)
    }

    log.Println("walk complete, total count", count)
}
