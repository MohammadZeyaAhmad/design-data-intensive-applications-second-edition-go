package modelbench

import (
	"context"
	"encoding/json"
	"sort"
	"sync/atomic"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres/detailsqlc"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type queryCounter struct{ count atomic.Int64 }
type countedDB struct {
	detailsqlc.DBTX
	count atomic.Int64
}

func (c *countedDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	c.count.Add(1)
	return c.DBTX.Query(ctx, sql, args...)
}

func (l *queryCounter) LogMode(logger.LogLevel) logger.Interface { return l }
func (l *queryCounter) Info(context.Context, string, ...any)     {}
func (l *queryCounter) Warn(context.Context, string, ...any)     {}
func (l *queryCounter) Error(context.Context, string, ...any)    {}
func (l *queryCounter) Trace(context.Context, time.Time, func() (string, int64), error) {
	l.count.Add(1)
}

type ormBrand struct {
	TenantID string `gorm:"primaryKey"`
	ID       string `gorm:"primaryKey"`
	Name     string
}

func (ormBrand) TableName() string { return "catalog.brands" }

type ormVariant struct {
	TenantID string
	SKU      string
	Code     string
	Label    string
	Position int
}

func (ormVariant) TableName() string { return "catalog.product_variants" }

type ormCategory struct {
	TenantID string
	SKU      string
	Category string
}

func (ormCategory) TableName() string { return "catalog.product_categories" }

type ormDetails struct {
	TenantID    string `gorm:"primaryKey"`
	SKU         string `gorm:"primaryKey"`
	BrandID     string
	Description string
	Attributes  []byte
	Version     int64
	Brand       ormBrand      `gorm:"foreignKey:TenantID,BrandID;references:TenantID,ID"`
	Variants    []ormVariant  `gorm:"foreignKey:TenantID,SKU;references:TenantID,SKU"`
	Categories  []ormCategory `gorm:"foreignKey:TenantID,SKU;references:TenantID,SKU"`
}

func (ormDetails) TableName() string { return "catalog.product_details" }

type Mapping struct {
	Adapter    string `json:"adapter"`
	Statements int64  `json:"statements"`
	Products   int    `json:"products"`
	WallNanos  int64  `json:"wall_nanos"`
	Digest     string `json:"digest"`
}

func mapping(ctx context.Context, pool *pgxpool.Pool, tenant string, skus []string) ([]Mapping, error) {
	start := time.Now()
	counted := &countedDB{DBTX: pool}
	rows, e := detailsqlc.New(counted).ListProductDetails(ctx, detailsqlc.ListProductDetailsParams{TenantID: tenant, Column2: skus})
	if e != nil {
		return nil, e
	}
	sqltime := time.Since(start).Nanoseconds()
	var want []domain.Details
	for _, r := range rows {
		d := domain.Details{SKU: r.Sku, BrandID: r.BrandID, BrandName: r.BrandName, Description: r.Description, Attributes: r.Attributes, Version: r.Version, Categories: r.Categories}
		if e = json.Unmarshal(r.Variants, &d.Variants); e != nil {
			return nil, e
		}
		want = append(want, d)
	}
	wantDigest := digest(want)
	if counted.count.Load() != 1 {
		return nil, errStatements
	}
	report := []Mapping{{"sqlc-1.30.0-pgx-join", counted.count.Load(), len(want), sqltime, wantDigest}}
	// A separate sql.DB owns only its connections to this isolated fixture DB.
	sqldb := stdlib.OpenDB(*pool.Config().ConnConfig)
	defer sqldb.Close()
	counter := &queryCounter{}
	db, e := gorm.Open(gormpg.New(gormpg.Config{Conn: sqldb}), &gorm.Config{Logger: counter, DisableAutomaticPing: true})
	if e != nil {
		return nil, e
	}
	for _, eager := range []bool{false, true} {
		counter.count.Store(0)
		var products []ormDetails
		start = time.Now()
		q := db.WithContext(ctx).Where("tenant_id = ? AND sku IN ?", tenant, skus).Order("sku")
		if eager {
			q = q.Preload("Brand").Preload("Variants", func(tx *gorm.DB) *gorm.DB { return tx.Order("position") }).Preload("Categories", func(tx *gorm.DB) *gorm.DB { return tx.Order("category") })
		}
		if e = q.Find(&products).Error; e != nil {
			return nil, e
		}
		if !eager {
			for i := range products {
				p := &products[i]
				if e = db.WithContext(ctx).Model(p).Association("Brand").Find(&p.Brand); e != nil {
					return nil, e
				}
				if e = db.WithContext(ctx).Model(p).Association("Variants").Find(&p.Variants); e != nil {
					return nil, e
				}
				if e = db.WithContext(ctx).Model(p).Association("Categories").Find(&p.Categories); e != nil {
					return nil, e
				}
			}
		}
		elapsed := time.Since(start).Nanoseconds()
		got := make([]domain.Details, 0, len(products))
		for _, p := range products {
			d := domain.Details{SKU: p.SKU, BrandID: p.BrandID, BrandName: p.Brand.Name, Description: p.Description, Attributes: json.RawMessage(p.Attributes), Version: p.Version, Variants: []domain.Variant{}, Categories: []string{}}
			sort.Slice(p.Variants, func(i, j int) bool { return p.Variants[i].Position < p.Variants[j].Position })
			for _, v := range p.Variants {
				d.Variants = append(d.Variants, domain.Variant{Code: v.Code, Label: v.Label})
			}
			for _, c := range p.Categories {
				d.Categories = append(d.Categories, c.Category)
			}
			sort.Strings(d.Categories)
			got = append(got, d)
		}
		if digest(got) != wantDigest {
			return nil, errMismatch
		}
		name := "gorm-1.31.1-lazy-associations"
		expected := int64(1 + 3*len(products))
		if eager {
			name = "gorm-1.31.1-eager-preload"
			expected = 4
		}
		if counter.count.Load() != expected {
			return nil, errStatements
		}
		report = append(report, Mapping{name, counter.count.Load(), len(got), elapsed, digest(got)})
	}
	return report, nil
}
