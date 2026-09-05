// 候选数据降级：原始排名保留，缺失不填值，账户权益仍严格阻断。
package strategy

import (
	"math"
	"sort"
)

// DataGap 描述加载范围内的一条不可用行情。
type DataGap struct {
	TsCode string `json:"ts_code"` // 证券。
	Date   string `json:"date"`    // 日期。
	Reason string `json:"reason"`  // 不可用原因。
}

// DataQuality 保存冻结数据策略和有界缺口报告，不代表全市场质量认证。
type DataQuality struct {
	Policy           string    `json:"policy"`            // strict或available_candidates。
	UnavailableRows  int       `json:"unavailable_rows"`  // 加载范围内不可用行数。
	SamplesTruncated bool      `json:"samples_truncated"` // 明细是否截断。
	Samples          []DataGap `json:"samples"`           // 最多100条，按证券/日期排序。
}

// PrepareReplayBars 严格模式保留原检查，受限模式移除非法行并保留缺口说明。
func PrepareReplayBars(bars map[string]map[string]Bar, dates []string, policy string) (DataQuality, error) {
	q := DataQuality{Policy: policy, Samples: []DataGap{}}
	if policy == "strict" {
		return q, ValidateReplayBars(bars)
	}
	if policy != "available_candidates" {
		return q, errInvalid("未知数据策略")
	}
	// Dates使用正式日历，逐日屏蔽不会利用之后是否补数来整只删除证券。
	for _, code := range sortedBarCodes(bars) {
		for _, date := range dates {
			b, ok := bars[code][date]
			reason := ""
			if !ok {
				reason = "缺行情记录，未确认为停牌"
			} else {
				for _, v := range []float64{b.Open, b.High, b.Low, b.Close, b.AdjFactor} {
					if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
						reason = "价格或复权因子非法"
						break
					}
				}
				if b.Amount < 0 || math.IsNaN(b.Amount) || math.IsInf(b.Amount, 0) ||
					b.Low > math.Min(b.Open, b.Close) || b.High < math.Max(b.Open, b.Close) {
					reason = "量价结构非法"
				}
			}
			if reason == "" {
				continue
			}
			delete(bars[code], date)
			q.UnavailableRows++
			if len(q.Samples) < 100 {
				q.Samples = append(q.Samples, DataGap{code, date, reason})
			}
		}
	}
	q.SamplesTruncated = q.UnavailableRows > len(q.Samples)
	return q, nil
}

// sortedBarCodes 稳定遍历缺口，避免map顺序影响报告和hash。
func sortedBarCodes(bars map[string]map[string]Bar) []string {
	codes := make([]string, 0, len(bars))
	for c := range bars {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	return codes
}
