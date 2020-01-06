#coding = 'utf-8'
import jieba
import jieba.analyse

def TF_IDF_cal(texts, topK = 100):
    return jieba.analyse.extract_tags(texts, topK=topK)

def cut_sentence(sentence, cut_type = 'sentence', seg_mark = ' '):
    if cut_type == 'list':
        return jieba.lcut(sentence)
    if cut_type == 'string':
        return seg_mark.join(jieba.cut(sentence))
    else:
        raise ValueError('unsupported cut type')