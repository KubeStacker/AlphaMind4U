const holdingMarketValue = (item, holdingsMap = {}) => {
  const shares = Number(holdingsMap?.[item?.ts_code]?.shares || 0);
  const price = Number(item?.price || 0);
  return shares * price;
};

export const sortHoldingRowsByValue = (rows = [], holdingsMap = {}) =>
  (Array.isArray(rows) ? rows : [])
    .filter((item) => Number(holdingsMap?.[item?.ts_code]?.shares || 0) > 0)
    .sort((a, b) => {
      const diff = holdingMarketValue(b, holdingsMap) - holdingMarketValue(a, holdingsMap);
      if (diff !== 0) return diff;
      return String(a?.ts_code || '').localeCompare(String(b?.ts_code || ''));
    });
