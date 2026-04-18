const firstLevel = (levels, support) =>
  (Array.isArray(levels) ? levels : []).find((level) => !!level && !!level.isSupport === support) || null;

export const buildZenFocusTokens = ({ levels = [], triggerText = '', invalidText = '' } = {}) => {
  const support = firstLevel(levels, true);
  const resistance = firstLevel(levels, false);
  const tokens = [];

  if (support?.priceText) {
    tokens.push({ key: 'support', label: '支', value: support.priceText, tone: 'support' });
  }
  if (resistance?.priceText) {
    tokens.push({ key: 'resistance', label: '压', value: resistance.priceText, tone: 'resistance' });
  }

  const trigger = String(triggerText || '').trim();
  if (trigger) {
    tokens.push({ key: 'trigger', label: '触', value: trigger, tone: 'trigger' });
  }

  const invalid = String(invalidText || '').trim();
  if (invalid) {
    tokens.push({ key: 'invalid', label: '失', value: invalid, tone: 'invalid' });
  }

  return tokens;
};
