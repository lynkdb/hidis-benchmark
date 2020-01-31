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
	"strings"

	"github.com/hooto/hflag4g/hflag"
	"github.com/lessos/lessgo/encoding/json"
	"github.com/lessos/lessgo/types"

	"github.com/hooto/hchart/v2/hcapi"
	"github.com/hooto/hchart/v2/hcutil"
)

type chartOptions struct {
	dataFiles             []string
	chartTitle            string
	chartName             string
	dataName              [][]string
	dataAttrGroup         [][]string
	dataAttrFilter        []string
	chartThroughputEnable bool
	chartLatencyEnable    bool
}

func matExp(ar0, ar1 [][]string) [][]string {

	if len(ar1) < 1 {
		return ar0
	}

	if len(ar0) < 1 {
		for _, v := range ar1[0] {
			ar0 = append(ar0, []string{v})
		}
	} else {

		ar := [][]string{}
		for _, v0 := range ar0 {
			for _, v1 := range ar1[0] {
				ar = append(ar, append(v0, v1))
			}
		}
		ar0 = ar
	}

	if len(ar1) > 1 {
		ar0 = matExp(ar0, ar1[1:])
	}

	return ar0
}

func newchartOptions() (*chartOptions, error) {

	it := &chartOptions{
		dataFiles: []string{"lynkbench.json"},
		chartName: "lynkbench",
	}

	if v, ok := hflag.ValueOK("data_file"); ok {
		it.dataFiles = strings.Split(v.String(), ",")
	}

	if v, ok := hflag.ValueOK("chart_title"); ok {
		it.chartTitle = v.String()
	}

	if v, ok := hflag.ValueOK("chart_name"); ok {
		it.chartName = v.String()
	}

	if v, ok := hflag.ValueOK("data_name"); ok {
		vArr := strings.Split(v.String(), "#")
		if n := len(vArr); n < 1 || n > 2 {
			return nil, errors.New("invalid --data_name")
		}
		it.dataName = [][]string{}
		for _, v2 := range vArr {
			v3 := strings.Split(v2, ",")
			if len(v3) < 1 || len(v3) > 4 {
				return nil, errors.New("invalid --data_name")
			}
			it.dataName = append(it.dataName, v3)
		}
		it.dataName = matExp(nil, it.dataName)
	}

	if v, ok := hflag.ValueOK("data_attr_filter"); ok {
		it.dataAttrFilter = strings.Split(v.String(), ",")
	}

	if v, ok := hflag.ValueOK("data_attr_group"); ok {
		vArr := strings.Split(v.String(), "#")
		if n := len(vArr); n < 1 || n > 2 {
			return nil, errors.New("invalid --data_attr_group")
		}
		it.dataAttrGroup = [][]string{}
		for _, v2 := range vArr {
			v3 := strings.Split(v2, ",")
			if len(v3) < 1 || len(v3) > 4 {
				return nil, errors.New("invalid --data_attr_group")
			}
			it.dataAttrGroup = append(it.dataAttrGroup, v3)
		}

		it.dataAttrGroup = matExp(nil, it.dataAttrGroup)
	}

	if _, ok := hflag.ValueOK("data_throughput_enable"); ok {
		it.chartThroughputEnable = true
	}

	if _, ok := hflag.ValueOK("data_latency_enable"); ok {
		it.chartLatencyEnable = true
	}

	return it, nil
}

func ChartOutput() error {

	opts, err := newchartOptions()
	if err != nil {
		return err
	}

	var ls hcapi.DataList

	for _, file := range opts.dataFiles {
		var obj hcapi.DataList
		if err := json.DecodeFile(file, &obj); err != nil {
			return err
		}
		for _, ds := range obj.Items {
			ds.AttrSet(ds.Name)
			ls.Set(ds)
		}
	}

	if err = chartThroughputAvgGroup(opts, ls); err != nil {
		fmt.Println(err)
	}

	if err = chartLatencyAvgGroup(opts, ls); err != nil {
		fmt.Println(err)
	}

	return nil
}

func chartThroughputAvgGroup(opts *chartOptions, ls hcapi.DataList) error {

	if !opts.chartThroughputEnable {
		return nil
	}

	if len(opts.dataAttrGroup) < 1 {
		return errors.New("no --data_attr_group found")
	}

	var (
		item = hcapi.ChartItem{
			Type: hcapi.ChartTypeBar,
		}
		sets = hcapi.DataList{}
	)

	if opts.chartTitle != "" {
		item.Options.Title = opts.chartTitle + " "
	}
	item.Options.Title += "Average Throughput (Queries Per Second)"
	item.Options.Y.Title = "Throughput (QPS)"

	for _, ds := range ls.Items {

		if !types.ArrayStringHas(ds.Attrs, "throughput") {
			continue
		}

		dnh := false
		for _, dnv := range opts.dataName {
			if types.ArrayStringHit(ds.Attrs, dnv) == len(dnv) {
				dnh = true
				break
			}
		}
		if !dnh {
			continue
		}

		if len(opts.dataAttrFilter) > 0 &&
			types.ArrayStringHit(ds.Attrs, opts.dataAttrFilter) != len(opts.dataAttrFilter) {
			continue
		}

		bgh := false
		for _, bgv := range opts.dataAttrGroup {
			if types.ArrayStringHit(ds.Attrs, bgv) == len(bgv) {
				bgh = true
				break
			}
		}
		if !bgh {
			continue
		}

		ds.AttrSet(ds.Name)

		sets.Set(ds)
	}

	item.Datasets = []*hcapi.DataItem{}
	for _, g := range opts.dataName {
		gds := hcapi.NewDataItem(strings.Join(g, "/"))
		for i := 0; i < len(opts.dataAttrGroup); i++ {
			gds.Points = append(gds.Points, &hcapi.DataPoint{
				Y: 0.0,
			})
		}
		item.Datasets = append(item.Datasets, gds)
	}

	for _, cg := range opts.dataAttrGroup {
		item.Labels = append(item.Labels, strings.Join(cg, "\n"))
	}

	for _, ds := range sets.Items {

		if len(ds.Points) < 1 {
			continue
		}
		qps := 0.0
		if p := ds.Points[len(ds.Points)-1]; p.X >= 1.0 {
			qps = float64(int64(p.Y) / int64(p.X))
		}

		for gi, g := range opts.dataName {

			if types.ArrayStringHit(ds.Attrs, g) != len(g) {
				continue
			}

			for cgi, cg := range opts.dataAttrGroup {
				if types.ArrayStringHit(ds.Attrs, cg) == len(cg) {
					item.Datasets[gi].Points[cgi].Y = qps
				}
			}
		}
	}

	return hcutil.Render(&item, &hcapi.ChartRenderOptions{
		Name:      opts.chartName + "_throughput_avg",
		SvgEnable: true,
	})
}

func chartLatencyAvgGroup(opts *chartOptions, ls hcapi.DataList) error {

	if !opts.chartLatencyEnable {
		return nil
	}

	if len(opts.dataAttrGroup) < 1 {
		return errors.New("no --data_attr_group found")
	}

	var (
		item = hcapi.ChartItem{
			Type: hcapi.ChartTypeBar,
		}
		sets = hcapi.DataList{}
	)

	if opts.chartTitle != "" {
		item.Options.Title = opts.chartTitle + " "
	}
	item.Options.Title += "Average Latency Time"
	item.Options.Y.Title = "Latency (us)"

	for _, ds := range ls.Items {

		if !types.ArrayStringHas(ds.Attrs, "latency-avg") {
			continue
		}

		dnh := false
		for _, dnv := range opts.dataName {
			if types.ArrayStringHit(ds.Attrs, dnv) == len(dnv) {
				dnh = true
				break
			}
		}
		if !dnh {
			continue
		}

		if len(opts.dataAttrFilter) > 0 &&
			types.ArrayStringHit(ds.Attrs, opts.dataAttrFilter) != len(opts.dataAttrFilter) {
			continue
		}

		bgh := false
		for _, bgv := range opts.dataAttrGroup {
			if types.ArrayStringHit(ds.Attrs, bgv) == len(bgv) {
				bgh = true
				break
			}
		}
		if !bgh {
			continue
		}

		ds.AttrSet(ds.Name)

		sets.Set(ds)
	}

	item.Datasets = []*hcapi.DataItem{}
	for _, g := range opts.dataName {
		gds := hcapi.NewDataItem(strings.Join(g, "/"))
		for i := 0; i < len(opts.dataAttrGroup); i++ {
			gds.Points = append(gds.Points, &hcapi.DataPoint{
				Y: 0.0,
			})
		}
		item.Datasets = append(item.Datasets, gds)
	}

	for _, cg := range opts.dataAttrGroup {
		item.Labels = append(item.Labels, strings.Join(cg, "\n"))
	}

	for _, ds := range sets.Items {

		if len(ds.Points) < 1 {
			continue
		}

		for gi, g := range opts.dataName {

			if types.ArrayStringHit(ds.Attrs, g) != len(g) {
				continue
			}

			for cgi, cg := range opts.dataAttrGroup {
				if types.ArrayStringHit(ds.Attrs, cg) == len(cg) {
					item.Datasets[gi].Points[cgi].Y = ds.Points[0].Y
				}
			}
		}
	}

	return hcutil.Render(&item, &hcapi.ChartRenderOptions{
		Name:      opts.chartName + "_latency_avg",
		SvgEnable: true,
	})
}
