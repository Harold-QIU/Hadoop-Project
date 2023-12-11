# README

## Description

2023年秋季学期分布式存储与计算Project

## Group Members

- 邱俊杰
- 胡强 

## Project Structure

`./data`: The data used in the project. `./data/order` and `./data/trade` should be stored the folder.

`./src`: The source code of the project.

`./README.md`: The README file of the project.

## Project TODO
 
- [ ] 在Sort阶段尝试Buffer IO读入数据，对比Tablesaw的读入速度，如若IO性能不高，我们可以放弃下面两条
- [ ] 实现单线程排序，并记录性能
- [ ] 实现多线程排序，对比单线程排序的效率
- [ ] 修改程序逻辑至97分以上
- [ ] 记录原有MapReduce每一个板块的性能指标
- [ ] 尝试使用StringBuilder来连接字符串

## Attention

- 所有没有order的cancel trade的ORDER_TYPE均标记为2