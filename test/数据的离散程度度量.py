import numpy as np
import stats as sts

scores = [20,21,22,23,24,25,26,27,32,31,30,29,28,27,26,25,24,23,22,21,20,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,2]
# 集中趋势的度量
print('求和：', np.sum(scores))
print('个数：', len(scores))
print('平均值:', np.mean(scores))
print('中位数:', np.median(scores))
# print('众数:', sts.mode(scores))
print('上四分位数', sts.quantile(scores, p=0.25))
print('下四分位数', sts.quantile(scores, p=0.75))
# 离散趋势的度量
print('最大值:', np.max(scores))
print('最小值:', np.min(scores))
print('极差:', np.max(scores) - np.min(scores))
print('四分位差', sts.quantile(scores, p=0.75) - sts.quantile(scores, p=0.25))
print('标准差:', np.std(scores))
print('方差:', np.var(scores))
print('离散系数:', np.std(scores) / np.mean(scores))
# 偏度与峰度的度量
print('偏度:', sts.skewness(scores))
print('峰度:', sts.kurtosis(scores))
