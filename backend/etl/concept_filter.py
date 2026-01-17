"""
概念板块过滤模块
用于过滤掉无实际意义的板块/概念数据
"""
from typing import Set

# 板块黑名单：包含所有无实际交易意义的指数/概念
CONCEPT_BLACKLIST: Set[str] = {
    # 技术性板块
    "融资融券", "转融券标的", "深股通", "沪股通", "标准普尔", "雄安新区",
    "富时罗素", "MSCI中国", "HS300_", "证金持股", "基金重仓", "一带一路",
    "创业板综", "深成500", "上证380", "中证500", "预盈预增", "专精特新", "自主可控", "华为产业链",
    "江苏板块", "浙江板块", "广东板块", "北京板块", "深圳特区", "A股", "含可转债",
    
    # 指数类
    "沪深300", "中证100", "中证200", "中证800", "中证1000",
    "上证50", "上证180", "上证综指", "深证100R", "深证300", "深证成指",
    "创业板指", "科创50", "北证50",
    
    # 机构持股类
    "QFII重仓", "社保重仓", "保险重仓", "券商重仓", "信托重仓",
    "QFII", "RQFII", "沪港通", "深港通", "港股通", "AH股",
    
    # 技术性概念
    "股权激励", "员工持股", "回购", "高送转", "ST板块",
    "次新股", "新股", "退市整理", "风险警示",
    "可转债", "债券", "权证", "期权", "期货",
    
    # 标普/富时概念
    "标普概念", "富时概念", "MSCI", "罗素", "道琼斯",
    "成份股", "样本股", "ETF", "LOF",
    
    # 其他无意义分类
    "2025三季报预增", "2024三季报预增", "2023三季报预增",
    "央企改革", "国企改革", "地方国企",

    # 垃圾分类
    "昨日连板_含一字", "昨日连板", "昨日触板", "举牌", "B股", "百元股", "2025三季报预减",
    
    # 概念类
    "中芯概念", "华为概念", "小米概念", "特斯拉概念"
}

# 技术性关键词（如果板块名称包含这些关键词，通常需要过滤）
TECH_KEYWORDS: Set[str] = {
    '融资融券', '转融券标的', '标准普尔', '富时罗素', 'MSCI中国', '证金持股',
    '基金重仓', 'QFII重仓', '社保重仓', '保险重仓', '券商重仓', '信托重仓',
    '深股通', '沪股通', '港股通', '沪港通', '深港通', 'AH股',
    '股权激励', '员工持股', '央企改革', '国企改革', '地方国企',
    '高送转', 'ST板块', '次新股', '退市整理', '风险警示',
    '可转债', '权证', '期权', '期货', 'QFII', 'RQFII',
    '标普概念', '富时概念', '道琼斯', '成份股', '样本股',
    '深成500', '上证380', '中证500', '深证100R', '深证300',
    '上证50', '上证180', '上证综指', '沪深300', '中证100',
    '中证200', '中证800', '中证1000', '创业板指', '科创50', '北证50',
    '预盈预增', 'MSCI', '罗素', 'ETF', 'LOF',
}

# 地域性关键词（如果板块名称只包含地域，通常需要过滤）
REGION_KEYWORDS: Set[str] = {
    '江苏', '浙江', '广东', '北京', '上海', '深圳', '天津', '重庆',
    '河北', '山西', '内蒙古', '辽宁', '吉林', '黑龙江',
    '安徽', '福建', '江西', '山东', '河南', '湖北', '湖南',
    '广西', '海南', '四川', '贵州', '云南', '西藏', '陕西',
    '甘肃', '青海', '宁夏', '新疆', '香港', '澳门', '台湾',
}

# 保留后缀（即使包含技术性关键词，但如果以这些后缀结尾，通常保留）
PRESERVE_SUFFIXES: Set[str] = {
    '行业', '概念', '板块',
}


def should_filter_concept(concept_name: str) -> bool:
    """
    判断是否应该过滤该概念板块
    
    Args:
        concept_name: 概念名称
        
    Returns:
        True表示应该过滤，False表示保留
    """
    if not concept_name:
        return True
    
    concept_name = concept_name.strip()
    
    # 过滤单字或过短的概念
    if len(concept_name) < 2:
        return True
    
    # 精确匹配黑名单
    if concept_name in CONCEPT_BLACKLIST:
        return True
    
    # 检查是否以保留后缀结尾
    has_preserve_suffix = any(concept_name.endswith(suffix) for suffix in PRESERVE_SUFFIXES)
    
    # 检查是否包含技术性关键词
    contains_tech_keyword = any(keyword in concept_name for keyword in TECH_KEYWORDS)
    
    # 如果包含技术性关键词但没有保留后缀，则过滤
    if contains_tech_keyword and not has_preserve_suffix:
        return True
    
    # 检查是否为纯地域性板块（只包含地域关键词+板块/概念）
    if concept_name.endswith('板块') or concept_name.endswith('概念'):
        base_name = concept_name[:-2]  # 去掉"板块"或"概念"
        if base_name in REGION_KEYWORDS:
            return True
    
    # 过滤包含年份的预增类概念（如"2025三季报预增"）
    if '预增' in concept_name or '预盈' in concept_name:
        # 检查是否包含年份
        import re
        if re.search(r'20\d{2}', concept_name):
            return True
    
    # 过滤纯数字或特殊字符过多的概念
    if len([c for c in concept_name if c.isdigit() or c in '_-']) > len(concept_name) * 0.5:
        return True
    
    return False


def filter_concept_list(concept_list: list) -> list:
    """
    过滤概念列表，移除无意义的概念
    
    Args:
        concept_list: 概念名称列表
        
    Returns:
        过滤后的概念列表
    """
    if not concept_list:
        return []
    
    filtered = []
    for concept in concept_list:
        if isinstance(concept, dict):
            # 如果是字典，尝试提取name字段
            concept_name = concept.get('name', concept.get('板块名称', ''))
        elif isinstance(concept, str):
            concept_name = concept
        else:
            continue
        
        if not should_filter_concept(concept_name):
            filtered.append(concept)
    
    return filtered
