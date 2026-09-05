// 错误构造：策略参数校验用，避免引入额外依赖。
package strategy

import "errors"

// errInvalid 参数非法错误。
func errInvalid(msg string) error { return errors.New("strategy: " + msg) }
