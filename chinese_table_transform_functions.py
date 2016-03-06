ChineseNumbers = {
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "零": 0
}
ChineseMultiplier = {
    "十": 10,
    "百": 100
}

def builder_ChineseFloorTransformer(ops):
    word = ops[0]
    num = 0
    coefficient = 1
    if word.find("地下") > 0:
        coefficient = -1
        word = word.replace("地下", "")
    for c in word:
        if c in ChineseNumbers:
            num += ChineseNumbers[c]
        elif c in ChineseMultiplier:
            if num == 0:
                num = ChineseMultiplier[c]
            else:
                num *= ChineseMultiplier[c]
        else:
            break
    return str(coefficient * num)
