import jieba


# 创建停用词list
def stopwords_list(file_path):
    stopwords = [line.strip() for line in open(file_path, 'r', encoding='utf-8').readlines()]
    return stopwords


# 对句子去除停用词
def remove_stop_words(sentence):
    stopwords = stopwords_list('d://temp//stop_words.txt')  # 这里加载停用词的路径
    outstr = ''
    sentence_seged = jieba.cut(sentence.strip())
    for word in sentence_seged:
        if word.isdigit():
            continue
        if word not in stopwords:
            if word != '\t':
                outstr += word
                outstr += " "
    return outstr



def main():
    file = open('d://temp//focus_news_20200208.csv', encoding="utf-8")
    txt = file.read()
    txt = remove_stop_words(txt)
    # words = jieba.lcut(txt)  #无空格
    # words = jieba.lcut(txt,cut_all=True)   #有空格
    words = jieba.lcut_for_search(txt)
    counts = {}
    for word in words:
        if len(word) == 1:
            continue
        else:
            counts[word] = counts.get(word, 0) + 1

    items = list(counts.items())

    items.sort(key=lambda x: x[1], reverse=True)
    # items.sort(reverse = True)
    for i in range(20):
        word, count = items[i]
        print(word, count)
    #    print('{0:<10}{1:>5}'.format(word,count))


if __name__ == '__main__':
    main()
