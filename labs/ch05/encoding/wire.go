package encoding

import (
	"encoding/xml"
	"sort"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
)

type line1 struct {
	SKU      string `json:"sku" xml:"sku" msgpack:"sku"`
	Quantity int32  `json:"quantity" xml:"quantity" msgpack:"quantity"`
}
type wire1 struct {
	XMLName   xml.Name `json:"-" msgpack:"-" xml:"snapshot"`
	Tenant    string   `json:"tenant_id" xml:"tenant_id" msgpack:"tenant_id"`
	ID        string   `json:"order_id" xml:"order_id" msgpack:"order_id"`
	Name      string   `json:"name" xml:"name" msgpack:"name"`
	Lines     []line1  `json:"lines" xml:"lines>line" msgpack:"lines"`
	Version   int64    `json:"version,string" xml:"version" msgpack:"version"`
	Expedited *bool    `json:"expedited,omitempty" xml:"expedited,omitempty" msgpack:"expedited,omitempty"`
	Legacy    string   `json:"legacy_note,omitempty" xml:"legacy_note,omitempty" msgpack:"legacy_note,omitempty"`
}
type wire2 struct {
	XMLName   xml.Name          `json:"-" msgpack:"-" xml:"snapshot"`
	Tenant    string            `json:"tenant_id" xml:"tenant_id" msgpack:"tenant_id"`
	ID        string            `json:"order_id" xml:"order_id" msgpack:"order_id"`
	Name      string            `json:"name" xml:"name" msgpack:"name"`
	Lines     []model.Line      `json:"lines" xml:"lines>line" msgpack:"lines"`
	Version   int64             `json:"version,string" xml:"version" msgpack:"version"`
	Expedited *bool             `json:"expedited,omitempty" xml:"expedited,omitempty" msgpack:"expedited,omitempty"`
	Currency  *string           `json:"currency,omitempty" xml:"currency,omitempty" msgpack:"currency,omitempty"`
	Discount  *int64            `json:"discount_minor,omitempty,string" xml:"discount_minor,omitempty" msgpack:"discount_minor,omitempty"`
	Labels    map[string]string `json:"labels,omitempty" xml:"-" msgpack:"labels,omitempty"`
}
type xmlLabel struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

// XMLFields is exported so encoding/xml can reflect through the embedded value.
type XMLFields wire2
type xml2 struct {
	XMLFields
	LabelList []xmlLabel `xml:"labels>label"`
}

func to1(r model.Record) wire1 {
	w := wire1{Tenant: r.Tenant, ID: r.ID, Name: r.Name, Version: r.Version, Expedited: r.Expedited, Legacy: r.LegacyNote, Lines: []line1{}}
	for _, l := range r.Lines {
		w.Lines = append(w.Lines, line1{l.SKU, int32(l.Quantity)})
	}
	return w
}
func to2(r model.Record) wire2 {
	return wire2{Tenant: r.Tenant, ID: r.ID, Name: r.Name, Lines: r.Lines, Version: r.Version, Expedited: r.Expedited, Currency: r.Currency, Discount: r.Discount, Labels: r.Labels}
}
func (w wire1) record() model.Record {
	r := model.Record{Tenant: w.Tenant, ID: w.ID, Name: w.Name, Version: w.Version, Expedited: w.Expedited, LegacyNote: w.Legacy, Lines: []model.Line{}}
	for _, l := range w.Lines {
		r.Lines = append(r.Lines, model.Line{SKU: l.SKU, Quantity: int64(l.Quantity)})
	}
	return r
}
func (w wire2) record() model.Record {
	return model.Record{Tenant: w.Tenant, ID: w.ID, Name: w.Name, Lines: w.Lines, Version: w.Version, Expedited: w.Expedited, Currency: w.Currency, Discount: w.Discount, Labels: w.Labels}
}
func toXML2(r model.Record) xml2 {
	w := xml2{XMLFields: XMLFields(to2(r))}
	keys := []string{}
	for k := range r.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w.LabelList = append(w.LabelList, xmlLabel{k, r.Labels[k]})
	}
	return w
}
func (w xml2) record() model.Record {
	r := wire2(w.XMLFields).record()
	if len(w.LabelList) > 0 {
		r.Labels = map[string]string{}
		for _, l := range w.LabelList {
			r.Labels[l.Key] = l.Value
		}
	}
	return r
}
