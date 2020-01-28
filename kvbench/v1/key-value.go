// Copyright 2020 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvbench

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/hooto/hflag4g/hflag"
	"github.com/lessos/lessgo/encoding/json"
	ps_cpu "github.com/shirou/gopsutil/cpu"

	"github.com/hooto/hchart/hcapi"
	"github.com/hooto/hchart/hcutil"
)

type KeyValueBenchWorker interface {
	Write(key, value []byte) ResultStatus
	Read(key []byte) ResultStatus
	Clean() error
}

type keyValueItem struct {
	Key, Value []byte
}

type keyValueWriteUsageItem struct {
	time int64
	num  int64
}

type keyValueBenchOptions struct {
	types          []uint64
	argsName       string
	timeLen        int64 // seconds
	timeStep       int64 // seconds
	keySize        int
	valSize        int
	clientNum      int64
	valSizeMin     int64
	valSizeMax     int64
	timeCostMin    int64 // microseconds
	timeCostMax    int64 // microseconds
	timeCostRanges []int64
	chartOutput    string
	chartTitle     string
	chartLegend    string
}

type KeyValueBench struct {
	options *keyValueBenchOptions
	items   []*keyValueBenchItem
}

func NewKeyValueBench() (*KeyValueBench, error) {

	opts, err := newKeyValueBenchOptions()
	if err != nil {
		return nil, err
	}

	return &KeyValueBench{
		options: opts,
	}, nil
}

func newKeyValueBenchOptions() (*keyValueBenchOptions, error) {

	it := &keyValueBenchOptions{
		types:       benchTypes(hflag.Value("bench_types").String()),
		timeLen:     10, // 10 s
		clientNum:   1,
		keySize:     40,
		valSize:     1 * 1024, // 1 KB
		valSizeMin:  0,
		valSizeMax:  0,
		timeCostMin: 10,    // 10 us
		timeCostMax: 100e3, // 100 ms
		chartOutput: "bench",
		chartTitle:  "",
		chartLegend: "",
	}

	if len(it.types) < 1 {
		return nil, errors.New("no --bench_types found")
	}

	if v, ok := hflag.ValueOK("time"); ok {
		if it.timeLen = v.Int64(); it.timeLen < 10 {
			it.timeLen = 10
		} else if it.timeLen > 600 {
			it.timeLen = 600
		}
	}

	if v, ok := hflag.ValueOK("key_size"); ok {
		if it.keySize = v.Int(); it.keySize < 16 {
			it.keySize = 16
		} else if it.keySize > 100 {
			it.keySize = 100
		}
	}

	if v, ok := hflag.ValueOK("value_size"); ok {
		if it.valSize = v.Int(); it.valSize < 1 {
			it.valSize = 1
		} else if it.valSize > (4 * 1024 * 1024) {
			it.valSize = 4 * 1024 * 1024
		}
	}

	if v, ok := hflag.ValueOK("time_cost_min"); ok {
		if it.timeCostMin = v.Int64(); it.timeCostMin < 1 {
			it.timeCostMin = 1 // 1 us
		} else if it.timeCostMin > 1e6 {
			it.timeCostMin = 1e6 // 1 s
		}
	}

	if v, ok := hflag.ValueOK("time_cost_max"); ok {
		it.timeCostMax = v.Int64()
	}
	if (it.timeCostMin * 10) > it.timeCostMax {
		it.timeCostMax = it.timeCostMin * 10
	}

	if v, ok := hflag.ValueOK("client_num"); ok {
		if it.clientNum = v.Int64(); it.clientNum < 1 {
			it.clientNum = 1
		} else if it.clientNum > 10000 {
			it.clientNum = 10000
		}
	}

	if v, ok := hflag.ValueOK("chart_output"); ok {
		it.chartOutput = v.String()
	}

	if v, ok := hflag.ValueOK("chart_title"); ok {
		it.chartTitle = v.String()
	}

	if v, ok := hflag.ValueOK("chart_legend"); ok {
		it.chartLegend = v.String()
	}

	// NPS
	it.timeStep = int64(1)
	if it.timeLen > 40 {
		it.timeStep = it.timeLen / 40
	}
	if fix := it.timeLen % it.timeStep; fix > 0 {
		it.timeLen += fix
	}

	// TC
	it.timeCostRanges = []int64{}
	timeCostRange := ((it.timeCostMax - it.timeCostMin) >> 20)
	if timeCostRange < it.timeCostMin {
		timeCostRange = it.timeCostMin
	}
	for i := 0; i < 20; i++ {

		if timeCostRange > it.timeCostMax {
			it.timeCostRanges = append(it.timeCostRanges, it.timeCostMax)
			break
		}

		it.timeCostRanges = append(it.timeCostRanges, timeCostRange)
		timeCostRange = timeCostRange << 1
	}

	return it, nil
}

func (it *KeyValueBench) Run(fn KeyValueBenchWorker) error {

	it.options.argsName = fmt.Sprintf("t%d_tc%d-%d",
		it.options.timeLen, it.options.timeCostMin, it.options.timeCostMax)

	for _, typ := range it.options.types {

		benchItem := newkeyValueBenchItem(it.options)
		benchItem.typ = typ

		var cio []float64
		for {
			cio, _ = ps_cpu.Percent(3e9, false)

			if (cio[0] / 10) < 1.0 {
				break
			}

			fmt.Printf("waiting %8.2f\r", cio[0]/10)
		}
		fmt.Printf("waiting %8.2f\n", cio[0]/10)

		if err := fn.Clean(); err != nil {
			return err
		}

		benchItem.name = it.options.chartLegend
		if benchItem.name != "" {
			benchItem.name += "/"
		}
		benchItem.name += benchTypeName(typ)

		if it.options.clientNum > 1 {
			benchItem.name += fmt.Sprintf("/client-x%d", it.options.clientNum)
		}

		fmt.Printf("Bench %s ...\r", benchItem.name)

		if err := benchItem.run(fn); err != nil {
			return err
		}

		if err := it.chartNumPerCycleLineSave(benchItem); err != nil {
			fmt.Println(err)
		}

		if err := it.chartTimeCostSave(benchItem); err != nil {
			fmt.Println(err)
		}

		fmt.Printf("Bench %s DONE at %s\n",
			benchItem.name, time.Now().Format("2006-01-02 15:04:05"))
	}

	return nil
}

func (it *KeyValueBench) chartNumPerCycleLineSave(benchItem *keyValueBenchItem) error {

	if it.options.chartOutput == "" {
		return nil
	}

	benchName := fmt.Sprintf("%s_%s.npc",
		it.options.chartOutput, it.options.argsName)

	//
	var item hcapi.ChartEntry
	if err := json.DecodeFile(benchName+".json", &item); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err := item.Valid(); err != nil {
			return err
		}
	}

	if item.Options.Title == "" && it.options.chartTitle != "" {
		item.Options.Title = it.options.chartTitle
	}

	item.Options.Title = fmt.Sprintf(
		"Queries Per Second (key-size %d, value-size %d)",
		it.options.keySize, it.options.valSize)

	item.Options.X.Title = "Seconds"
	item.Options.Y.Title = "Queries Per Second"

	if len(benchItem.status.npsMap) > 0 {

		for i := len(benchItem.status.npsMap) - 1; i > 0; i-- {
			benchItem.status.npsMap[i].num =
				(benchItem.status.npsMap[i].num - benchItem.status.npsMap[i-1].num) / it.options.timeStep
		}
		benchItem.status.npsMap[0].num = benchItem.status.npsMap[0].num / it.options.timeStep

		item.Type = hcapi.ChartTypeLine

		p := item.Dataset(benchItem.name)
		p.Points = nil

		for _, v := range benchItem.status.npsMap {
			p.Points = append(p.Points, &hcapi.ChartPoint{
				X: float64(v.time),
				Y: float64(v.num),
			})
		}
	}

	if err := json.EncodeToFile(item, benchName+".json", "  "); err != nil {
		return err
	}

	return hcutil.Render(&item, &hcapi.ChartRenderOptions{
		Name: benchName,
	})
}

func (it *KeyValueBench) chartTimeCostSave(benchItem *keyValueBenchItem) error {

	if it.options.chartOutput == "" {
		return nil
	}

	benchName := fmt.Sprintf("%s_%s.tc",
		it.options.chartOutput, it.options.argsName)

	//
	var item hcapi.ChartEntry
	if err := json.DecodeFile(benchName+".json", &item); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err := item.Valid(); err != nil {
			return err
		}
	}

	if item.Options.Title == "" && it.options.chartTitle != "" {
		item.Options.Title = it.options.chartTitle
	}

	item.Options.Title = fmt.Sprintf(
		"Percentage of the queries served within a certain time (key-size %d, value-size %d)",
		it.options.keySize, it.options.valSize)

	item.Options.X.Title = "Latency Time"
	item.Options.Y.Title = "Percentage of Time-Cost (%)"

	if n := len(benchItem.status.timeCostMap); n > 0 {

		item.Type = hcapi.ChartTypeBar
		item.Data.Labels = []string{}

		p := item.Dataset(benchItem.name)

		for _, v := range benchItem.status.timeCostMap {
			if v.time > 1e6 {
				item.Data.Labels = append(item.Data.Labels, fmt.Sprintf("%d s", v.time/1e6))
			} else if v.time > 1e3 {
				item.Data.Labels = append(item.Data.Labels, fmt.Sprintf("%d ms", v.time/1e3))
			} else {
				item.Data.Labels = append(item.Data.Labels, fmt.Sprintf("%d us", v.time))
			}

			p.Values = append(p.Values,
				float64Round(float64(100*v.num)/float64(benchItem.status.ok+benchItem.status.err), 4))
		}
	}

	if err := json.EncodeToFile(item, benchName+".json", "  "); err != nil {
		return err
	}

	return hcutil.Render(&item, &hcapi.ChartRenderOptions{
		Name: benchName,
	})
}

/**
func (it *KeyValueBench) chartNumPerCycleSave(benchItem *keyValueBenchItem) error {

	if it.options.chartOutput == "" {
		return nil
	}

	benchName := fmt.Sprintf("%s_%s.npc",
		it.options.chartOutput, it.options.argsName)

	//
	var item hcapi.ChartEntry
	if err := json.DecodeFile(benchName+".json", &item); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err := item.Valid(); err != nil {
			return err
		}
	}

	if item.Options.Title == "" && it.options.chartTitle != "" {
		item.Options.Title = it.options.chartTitle
	}

	item.Options.Title = fmt.Sprintf(
		"Queries Per Second (key-size %d, value-size %d)",
		it.options.keySize, it.options.valSize)

	item.Options.X.Title = "Seconds"
	item.Options.Y.Title = "Queries Per Second"

	if len(benchItem.status.npsMap) > 0 {

		for i := len(benchItem.status.npsMap) - 1; i > 0; i-- {
			benchItem.status.npsMap[i].num =
				(benchItem.status.npsMap[i].num - benchItem.status.npsMap[i-1].num) / it.options.timeStep
		}
		benchItem.status.npsMap[0].num = benchItem.status.npsMap[0].num / it.options.timeStep

		item.Type = hcapi.ChartTypeBar
		item.Data.Labels = []string{}

		p := item.Dataset(benchItem.name)

		for _, v := range benchItem.status.npsMap {
			item.Data.Labels = append(item.Data.Labels, fmt.Sprintf("%d", v.time))
			p.Values = append(p.Values, float64(v.num))
		}
	}

	if err := json.EncodeToFile(item, benchName+".json", "  "); err != nil {
		return err
	}

	return hcutil.Render(&item, &hcapi.ChartRenderOptions{
		Name: benchName,
	})
}
*/
