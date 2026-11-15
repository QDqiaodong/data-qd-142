# Categorical drift accounting v1

供数据监测分析者比较两个聚合频数快照。categories为1..1000个唯一类别，reference/current各最多1000个category/count，唯一且必须在声明域内；缺行表示该声明类别频数0，而非未知漏采。count严格0..10^9整数，每快照总数>0。alpha为1..10^6正整数平滑伪计数。
对所有声明类别做加alpha平滑，p=(reference_count+alpha)/(reference_total+alpha*K)，q同理。输出原频数、精确分数p/q与q-p；PSI=sum((q-p)*ln(q/p))，Jensen-Shannon用自然对数、m=(p+q)/2，JS=(KL(p||m)+KL(q||m))/2。新出现/消失类依据原始0/正频数区分，双0类仍占平滑域。
样例总量各10，平滑a为9/13→3/13、b相反、c为1/13→1/13。相同分布PSI/JS为0。浮点散度非显著性、漂移阈值或根因结论，样本与聚合覆盖应由调用方核实；不处理连续特征分箱或在线告警。
