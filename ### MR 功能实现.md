### MR 功能实现
1.  有一个大处理的大数据,被划分为大小相同的数据块,以及相应的作业程序
2.  实现一个MR库
3.  将输入文件分布为M个数据块
4.  由DoMap进行出力,生成结果的中间文件
5.  DoMap处理完, 由DoReduce 进行出力,并生成最终结果


### 实现流程
1. 顺序实现
2. 并发实现
3. 