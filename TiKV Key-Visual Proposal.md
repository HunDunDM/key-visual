<!--
This is a template for TiDB's change proposal process, documented [here](./README.md).
-->

# Proposal: <Key Visual>

- Author(s):     <!-- Author Name, Co-Author Name, with the link(s) of the GitHub profile page -->         Xiangsheng Zheng([https://github.com/hundundm](https://github.com/hundundm)), Wei Tao([https://github.com/Sullivan12138](https://github.com/Sullivan12138)), Zhanhe Zhong([https://github.com/zzh-wisdom](https://github.com/zzh-wisdom))
- Last updated:  2019/10/22
- Discussion at: [Google Doc](https://docs.google.com/document/d/1vKAiBK-7g0EMXxM98tLNGRnCC62cu6-A84RrL-ENzcU/)

## Abstract

<!--
A short summary of the proposal:
- What is the issue that the proposal aims to solve?
- What needs to be done in this proposal?
- What is the impact of this proposal?
-->
- This proposal is intended to show an intuitive image of read and write statistics in regions.
- In this proposal, the backend needs a server which can get data from PD, calculate it and return the result to frontend, the frontend should draw a heatmap and support good interaction experience.
- This proposal can help the DBA to find those hot spots in regions, and also the distribution of data requests.

## Background

<!--
An introduction of the necessary background and the problem being solved by the proposed change:
- The drawback of the current feature and the corresponding use case
- The expected outcome of this proposal.
-->
In the past, there was no hot spots monitor that can draw a map in PD, therefore we are hard to find intuitively some areas that have large amount of data read or written. 
As we all know, high frequency of requests aimed at certain area is very likely to cause perfomance loss, bigger latency, or even hardware malfunction. A DBA may want to use hot spots monitor in such cases:
- find error cause in a database
- judge the perfomance of a database
- calculate request amount of a database
- analysis the distribution of database
- ...

This proposal is expected to help you easily find those regions having high frequency of data requests. You can use it at any time to check the current TiDB status , and you can even see the historic information during one year. Once you find a hot spot you may take some measures to eliminate such situations, like split regions or cut down requests.


## Proposal


<!--
A discussion of alternate approaches and the trade-offs, advantages, and disadvantages of the specified approach:
- How other systems solve the same issue?
- What other designs have been considered and what are their disadvantages?
- What is the advantage of this design compared with other designs?
- What is the disadvantage of this design?
- What is the impact of not doing this?
-->
### Google Key-Visualizer

Google has also designed such a tool called Key Visualizer, which can detect the data requests amount of keys in a database. That tool looks very like this proposal, but its granularity is  key level, ours is region level. Such thin granularity can reserve data percision maximally, however, it is not suitable for TiDB, for we cannot get information of every key from DB, what we can get is the overall statistics in one region. Although this design using region as elementary uint can inevitably cause percision loss, we think such loss is tolerable considering its benefit.
## Compatibility

<!--
A discussion of the change with regard to the compatibility issues:
- Does this proposal make TiDB not compatible with the old versions?
- Does this proposal make TiDB more compatible with MySQL?
-->
Our product will be merged into PD and provide API eventually, it is sure that it will not cause any conflict with TiDB. As for the improvement of the compatiblity with MySQL, now there are not any proofs indicate it, and we should wait for test after merging to check this metric.
## Implementation

<!--
A detailed description for each step in the implementation:
- Does any former steps block this step?
- Who will do it?
- When to do it?
- How long it takes to accomplish it?
-->
- Steps:
    - Design of the backend framework
    - Implementation of functions in backend
    - Design and implementation of frontend
    - test perfomance on the server
    - Merge the tool into PD
- Assignments
    - Xiangsheng Zheng build the framework and start server
    - Wei Tao writes the updating data part
    - Zhanhe Zhong writes the handling https requests part
    - Write the frontend together
    - Test the perfomance together
- Timeline
    - Week1: Take TiKV offline courses, get familiar with TiKV and PD, read and understand Key-Visual Demo
    - Week2: Write the backend and frontend of Key-Visual
    - Week3: Merge our code into PD


## Open issues (if applicable)

<!--
A discussion of issues relating to this proposal for which the author does not know the solution. This section may be omitted if there are none.
-->
- When there are too many regions, how to reduce the usage of the memory.
- How to persist data.
- How to reduce data percision loss when generating heatmaps.