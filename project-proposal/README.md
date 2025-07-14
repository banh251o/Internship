TỐI ƯU HOÁ EMR CLUSTER BẰNG SPOT INSTANCES & AUTO SCALING
Đề xuất giải pháp giảm chi phí và nâng cao hiệu suất vận hành hạ tầng Big Data trên AWS

---

Tổng quan
Trong bối cảnh nhu cầu xử lý dữ liệu lớn (Big Data) ngày càng tăng, các doanh nghiệp đang đối mặt với bài toán tối ưu hoá hạ tầng tính toán sao cho vừa đảm bảo khả năng xử lý khối lượng dữ liệu khổng lồ, vừa kiểm soát được chi phí vận hành. Amazon EMR (Elastic MapReduce) từ lâu đã là lựa chọn phổ biến để chạy các workload Spark, Hadoop trên đám mây AWS. Tuy nhiên, việc triển khai EMR theo phương thức On-Demand truyền thống dẫn đến chi phí cao, đặc biệt khi phải duy trì cluster lớn, phục vụ các job ETL (Extract – Transform – Load) hằng ngày.
Đề xuất này tập trung vào việc tái cấu trúc mô hình vận hành EMR Cluster bằng cách áp dụng Spot Instances, Auto Scaling, cùng các chính sách lập lịch workload linh hoạt, giải pháp giám sát (Monitoring) và quy trình vận hành chuẩn hoá (Operational Procedures). Mục tiêu là đạt mức tiết kiệm chi phí trên 50%, đồng thời duy trì hiệu năng xử lý ổn định, khả năng mở rộng linh hoạt, đảm bảo SLA (Service Level Agreement) với các nhóm phân tích dữ liệu (Data Engineering, BI).

Vấn đề cốt lõi
Hiện tại, cluster EMR đang được chạy hoàn toàn bằng On-Demand Instances, thiếu khả năng mở rộng tự động và phụ thuộc nhiều vào thao tác thủ công của đội ngũ vận hành. Điều này dẫn tới:
Chi phí duy trì trung bình 50-100 USD/tháng.
Tỷ lệ sử dụng tài nguyên (CPU/Memory) không đều, gây lãng phí.
Rủi ro quá tải hoặc thiếu tài nguyên khi workload đột biến.
Thiếu hệ thống giám sát chi tiết, khó phát hiện sớm các bottleneck.

Giải pháp đề xuất
 Sử dụng Spot Instances:
	 Tận dụng Spot Instances để chạy hạ tầng thử nghiệm nhỏ, thay vì On-Demand 100%.
	 Với quy mô cluster 2–5 nodes, sinh viên có thể dễ dàng khởi tạo Spark Job, ETL Pipeline với chi phí thấp, hoặc thậm chí miễn phí nếu tận dụng các khoản AWS Credits dành cho sinh viên.

Lợi ích kỳ vọng
Tiết kiệm >50% chi phí hạ tầng demo, duy trì chi phí dưới ~40 USD/tháng.
Minh chứng khả năng tự động scale cluster nhỏ, thay đổi số node linh hoạt theo workload.
Tăng khả năng làm chủ kiến thức: Spot Instance, Auto Scaling Policy, CloudWatch.
Làm PoC (Proof of Concept) để trình bày cho nhà trường hoặc mentor.



Đầu tư & Timeline
Chi phí hạ tầng mục tiêu: ~20–40 USD/tháng.
Thời gian thực hiện: ~4–6 tuần, chia 3 giai đoạn:
Thiết kế kiến trúc, chuẩn bị dataset.
Triển khai cluster nhỏ với Spot + Auto Scaling.
Chạy job test, benchmarking, viết báo cáo best practices.
Chỉ số thành công 
Cost Saving: So với baseline On-Demand ~80–100 USD/tháng, chi phí Spot giữ ~20–40 USD/tháng → tiết kiệm 50–70%.
Performance: Cluster demo uptime >95%.
Scalability: Tự động scale 1–5 nodes tuỳ tải.

# 1. Problem Statement
1.1. Current Situation
Hệ thống cluster Big Data thử nghiệm hiện tại chạy hoàn toàn bằng On-Demand Instances. Quy mô nhỏ (~2–5 nodes) nhưng sinh viên thường để cluster chạy cố định, không tắt hoặc scale down khi không dùng, dẫn đến phát sinh chi phí không cần thiết (~80–100 USD/tháng).
 Hơn nữa, các chính sách auto scaling chưa được áp dụng, Spot Instances chưa tận dụng vì thiếu hiểu biết về cách thiết lập Spot Pools, cách xử lý gián đoạn (Spot Interruption).
 
 1.2. Key Challenges
Ngân sách sinh viên hạn chế.
Khó theo dõi chi phí phát sinh nếu không quản lý tự động.
Thiếu kinh nghiệm cấu hình Spot Instances, Auto Scaling.
Thiếu dashboard giám sát chi tiết (CloudWatch).

1.3. Stakeholder Impact
Sinh viên: Khó kiểm soát chi phí, dễ dùng quá quota AWS Free Tier → phát sinh phí.
Mentor: Muốn thấy sinh viên có khả năng tối ưu tài nguyên cloud.

1.4. Business Consequences
Chi phí cloud tăng ngoài ý muốn.
Mất cơ hội học tập thực tế về Spot, Auto Scaling.
Thiếu dữ liệu benchmark để minh chứng đề tài.

1.5. Market Oportunity
Spot Instances có thể tiết kiệm đến 70–90% so với On-Demand.
AWS Free Tier + AWS Educate hỗ trợ tín chỉ miễn phí.
Dễ dàng mở rộng mô hình demo thành mini PoC để báo cáo hoặc thuyết trình.

# 2. Solution Architecture
2.1. Architecture Overview
Giải pháp tối ưu hoá EMR Cluster sẽ được triển khai theo kiến trúc linh hoạt, kết hợp Spot Instances với On-Demand Instances để cân bằng giữa chi phí và độ ổn định. Cluster sẽ được thiết kế theo mô hình multi-node, gồm:
Master Node: Chạy On-Demand để đảm bảo tính liên tục quản lý cluster.


Core/Task Nodes: Chạy chủ yếu bằng Spot Instances, cấu hình Auto Scaling để tăng/giảm số lượng node tuỳ theo tải thực tế.


Dữ liệu nguồn (input datasets) được lưu trữ trên Amazon S3, đầu ra (output) cũng trả về S3 hoặc Data Lake để phân tích tiếp bằng Athena hoặc các công cụ BI khác.
Mô hình tích hợp với các dịch vụ AWS khác như CloudWatch để giám sát, IAM để kiểm soát truy cập, đảm bảo tính bảo mật end-to-end.
Sơ đồ tổng quan kiến trúc:
(Vẽ sơ đồ: S3 → EMR Master Node (On-Demand) → Core/Task Nodes (Spot) → S3 Output → CloudWatch)

2.2. AWS Services Used
Dịch vụ                                                       Vai trò
Amazon EMR                                       Cluster Spark/Hadoop xử lý Big Data
EC2 Spot Instances                                Chạy Core/Task Nodes chi phí thấp
EC2 On-Demand                                     Chạy Master Node, đảm bảo quản lý
S3                                                 Lưu trữ dữ liệu nguồn & kết quả
Auto Scaling Group                                   Tự động mở rộng/thu hẹp node
CloudWatch                                    Giám sát CPU, task pending, sự kiện reclaim
IAM                                              Quản lý truy cập, role cho EMR và EC2
Athena/Glue                                       Tích hợp phân tích dữ liệu đầu ra

2.3. Component Design
Input Layer: Dataset tải lên S3, chia theo partition.
Processing Layer: EMR chạy Spark job/ETL pipeline.
Scaling Mechanism: Auto Scaling dựa trên CPU Utilization, Task Pending Metrics.
Failover: Spot bị reclaim dẫn đến fallback On-Demand Pool hoặc scale thêm node khác.
Output Layer: Kết quả ghi về S3 bucket, tuỳ chọn trigger Athena.

2.4. Security Architecture
Data Encryption: Bật encryption at rest (S3 SSE-S3/SSE-KMS) và in transit (TLS).
Access Control: Sử dụng IAM Roles cho EMR, hạn chế permission chỉ đúng mức cần thiết.
Cluster Permissions: Chỉ cho phép nhóm DevOps/Data Engineer truy cập key cluster config.
Monitoring Logs: Ghi log cluster vào S3, CloudWatch Logs để trace audit trail.

2.5. Scalability Design
Auto Scaling Policy:
Scale-out khi CPU > 70% hoặc số pending tasks > ngưỡng.
Scale-in khi CPU < 30% hoặc workload giảm.
Spot Fleet Strategy: Triển khai nhiều loại Instance Type (Diversified Pool) để tránh thiếu capacity.
On-Demand Fallback: Khi Spot bị reclaim, kích hoạt On-Demand để duy trì workload quan trọng.
Testing: Benchmark tải peak, kiểm tra khả năng scale từ 2 → 5 nodes.

# 3. Technical Implementation
3.1. Implementation Phases
Phase 1: Đánh giá & Thiết kế
Phân tích workload ETL hiện tại.
Chọn Instance Type phù hợp.
Tính toán chi phí Spot/On-Demand.
Thiết kế kiến trúc & policy scaling.

Phase 2: Cài đặt & Triển khai
Khởi tạo EMR Cluster với cấu hình mixed On-Demand + Spot.
Thiết lập Auto Scaling Groups.
Tạo CloudWatch Dashboard + Alarms.
Tích hợp IAM Roles, S3 buckets.

Phase 3: Benchmark & Tinh chỉnh
Chạy Spark jobs với dataset giả lập.
Ghi log performance, chi phí tiêu thụ.
Điều chỉnh threshold scale-out/in.
Hoàn thiện best practices.

3.2. Technical Requirements
Compute: 1 Master Node (On-Demand), 2–5 Core Nodes (Spot).
Instance Types: ví dụ m5.xlarge, m5.2xlarge (có thể thử r5 nếu workload nặng RAM).
Storage: Input/Output trên S3, EMR local storage tối thiểu 50 GB/node.
Network: Private Subnet, VPC bảo mật, cấu hình Security Group cho EMR.
Permissions: Tạo các IAM Role: EMR_EC2_DefaultRole, EMR_DefaultRole.

3.3. Development Approach
Hạ tầng: Dùng AWS Console hoặc IaC (Infrastructure as Code) – Terraform/CloudFormation.
Quản lý Version: Sử dụng Git/GitHub lưu scripts config cluster.
Pipeline: Tích hợp CI/CD nếu có thể, để deploy config tự động.
Tài liệu: Viết đầy đủ hướng dẫn khởi tạo, teardown cluster.

3.4. Testing Strategy
Unit Test: Kiểm tra script deploy EMR cluster, policy scaling.
Integration Test: Chạy Spark job end-to-end, validate dữ liệu output.
Performance Test: Benchmark tốc độ xử lý dataset 10–50 GB.
Interruption Test: Mô phỏng Spot bị reclaim, quan sát failover.
Monitoring: Xem alert/metric trên CloudWatch Dashboard.

3.5. Deployment Plan
Triển khai Pilot: 1–2 tuần, cluster nhỏ chạy dataset test.
Triển khai Chính Thức: Khi test đạt KPI cost saving & uptime.
Rollback: Giữ config On-Demand cluster dự phòng.
Maintenance: Định kỳ review cost + policy scaling.
Knowledge Transfer: chia sẻ cho nhóm.

4.1. Project Timeline
Giai đoạn                                             Công việc                                             Thời gian
Week 1–2                                 Phân tích hiện trạng, thiết kế kiến trúc                            2 tuần
Week 3–4                      Triển khai cluster test, thiết lập Spot Pools, Auto Scaling Policy             2 tuần
Week 5                                   Benchmark, chạy Spark Job, tuning policy                            1 tuần
Week 6                                 Hoàn thiện tài liệu vận hành, nghiệm thu PoC                          1 tuần

4.2. Key Milestones
 Hoàn thành kiến trúc & thiết kế Spot strategy
 Cluster test chạy ổn định >95% uptime
 Chứng minh tiết kiệm >50% so với On-Demand
 Tài liệu runbook + Best Practices hoàn chỉnh
 
4.3. Dependencies
Dataset test có sẵn.
Quyền truy cập AWS Free Tier hoặc Credits.
Đường truyền Internet ổn định.
Tài khoản IAM đủ quyền tạo EC2, EMR.

4.4. Resource Allocation
Nhân lực: 1 sinh viên DevOps/Data Engineer.
Mentor: Hỗ trợ review kiến trúc & chi phí.
Tài nguyên AWS: 1 account, giới hạn quota EC2 & S3.

5. Budget Estimation
5.1. Infrastructure Costs
Tính toán chi phí thực tế
Giả định:
Region: ap-southeast-1 (Singapore)
Instance Type: m5.xlarge (4 vCPU, 16GB RAM)
Spot Price (average): ~0.036 USD/vCPU-Hour → ~0.145 USD/hour/node
 (Nguồn: https://aws.amazon.com/ec2/spot/pricing/)
On-Demand Price: ~0.204 USD/hour/node
 (Nguồn: AWS Singapore On-Demand Pricing)
Cluster giả định:
1 Master Node: On-Demand (24/7)
2–5 Core Nodes: Spot (Auto Scaling)
Tính chi phí hàng tháng:
Thành phần         Loại         Giá/H            Số giờ/tháng                  Số node         Tổng
Master Node      On-Demand     0.204 USD            ~720                          1          ~147 USD
Core Nodes         Spot        0.145 USD    ~360 (average 50% uptime)            ~3          ~156 USD
Tổng compute                                                                                 ~303 USD/tháng

S3 Storage & Data Transfer
Dung lượng: ~50 GB input/output.
S3 Standard Storage Singapore: ~0.023 USD/GB.
Tổng chi phí lưu trữ: ~1.15 USD/tháng.
Data Transfer Out (<1TB): Miễn phí mức thấp → bỏ qua.

CloudWatch & Monitoring
~5 GB Logs/tháng → 0.5 USD.
~1 Dashboard & 5 Custom Metrics: ~3–5 USD/tháng.

Tổng hạ tầng (ước tính)
Hạng mục                               Chi phí/tháng         
Compute (Spot + On-Demand)               ~303 USD  
S3 Storage                               ~1.15 USD
Monitoring                               ~5 USD
Tổng chi phí                          ~310 USD/tháng
Ghi chú: Có thể sử dụng AWS Free Tier, hoặc AWS Educate Credits (~100–200 USD) để giảm chi phí thực tế.

5.2. Development Costs
Không thuê ngoài → 0 USD.
Chi phí: Thời gian tự học + công sức tự làm.

5.3. Operational Costs
Tương lai: Nếu mở rộng production, chi phí Cloud Support, Monitoring nâng cao, Data Transfer Out >1TB mới phát sinh (~0.09 USD/GB).

5.4. ROI Analysis
So sánh                  On-Demand 100%                  Spot + Auto Scaling
Compute                ~550–600 USD/tháng                  ~310 USD/tháng
Tiết kiệm                      -                                ~50%
Lợi ích          Giảm phí, tăng kiến thức, scale dễ

 PoC thành công -> mở rộng cho Production scale-out -> tiết kiệm 1,000–5,000 USD/năm tuỳ quy mô.
6. Risk Assessment
6.1. Risk Matrix
Rủi ro                              Xác suất      Tác động         Mức độ
Spot bị reclaim                       Cao        Trung bình         Cao
Chi phí phát sinh ngoài dự tính   Trung bình     Trung bình      Trung bình
Thiếu quota EC2                      Thấp           Cao          Trung bình
Thiếu kỹ năng scale/rollback      Trung bình        Cao             Cao

6.2. Mitigation Strategies
Spot Reclaim: Luôn thiết lập On-Demand fallback.
Quota: Kiểm tra quota trước khi deploy.
Chi phí: Thiết lập budget alert trên AWS Billing.
Thiếu kỹ năng: Viết runbook, thực hành benchmark trước.

6.3. Contingency Plans
Nếu Spot fail → Auto Scaling Group spin up On-Demand.
Nếu vượt budget → Scale down node, tạm ngưng job.
Nếu lỗi config → Rollback cluster config gốc.

7. Expected Outcomes
7.1. Success Metrics
Tiết kiệm tối thiểu 50% so với chạy On-Demand.
Cluster uptime >95%.
Job Spark ETL chạy thành công với dataset 10–50 GB.
Alert hoạt động, metric chính xác.

7.2. Business Benefits
Mô hình PoC khả thi để mở rộng quy mô doanh nghiệp.
Sinh viên thành thạo Spot, Auto Scaling, Monitoring.
Chuẩn hoá tài liệu chia sẻ cho nhóm.

7.3. Technical Improvements
Áp dụng kỹ năng hạ tầng thực tế.
Biết cách benchmark performance.
Tự động scale workload — giảm thao tác thủ công.

7.4. Long-term Value
Triển khai chuẩn có thể mở rộng thành Production Cluster.
Quy trình Best Practices có thể tái sử dụng.
Chứng minh năng lực vận hành Big Data trên AWS.

Appendices
A. Technical Specifications
Instance: m5.xlarge (4 vCPU, 16GB RAM)
Region: ap-southeast-1 (Singapore)
Storage: S3 Standard, SSE-KMS.

B. Cost Calculations
1. Thông tin cơ bản
Thông số	                              Giá trị
Region	                    ap-southeast-1 (Singapore)
Instance Type             Master	m5.xlarge (4 vCPU, 16GB RAM)
Instance Type Core/Task	           m5.xlarge (Spot)
On-Demand giá/h (Master)	         ~0.204 USD/giờ
Spot giá/h (Core/Task)	            ~0.145 USD/giờ
S3 Storage	                      0.023 USD/GB/tháng
CloudWatch Logs	                  ~0.50 USD/GB
CloudWatch Metrics	        ~0.30 USD/Custom Metric/tháng
CloudWatch Dashboard	           ~3 USD/Dashboard/tháng

2️. Tính chi phí Compute
Thành phần	      Loại	   Instance	   Số node  Số giờ/tháng   Giá mỗi giờ	   Tổng
Master Node	    On-Demand	m5.xlarge	   1	       720	       0.204 USD	146.88 USD
Core Nodes	      Spot	   m5.xlarge  Trung bình  360 (do scale 0.145 USD	156.60 USD
                                        3 nodes     theo tải)
Tổng Compute: ~303.48 USD/tháng

3️. Tính chi phí S3 Storage
Loại	         Dung lượng	   Giá/GB/tháng	   Tổng
Input Data	      30 GB	        0.023 USD	    0.69 USD
Output Data	      20 GB	        0.023 USD	    0.46 USD
Logs	            ~5 GB	        0.023 USD	    0.12 USD
Tổng S3 Storage	55 GB		                ~1.27 USD/tháng

4️. CloudWatch Monitoring
Loại	         Số lượng	      Giá	        Tổng
Logs Storage	~5 GB	      0.50 USD/GB	   2.50 USD
Dashboard	     1	        3 USD	        3 USD
Custom Metrics	  5	     0.30 USD/metric  1.50 USD
Tổng Monitoring			                  ~7 USD/tháng

5️. Dự phòng Data Transfer
Loại	                     Dung lượng	      Giá	         Tổng
Data Transfer Out (<1TB)	Miễn phí	          0	            0 USD

TỔNG TẤT CẢ
Hạng mục	                        Ước tính chi phí
Compute (Spot + On-Demand)         	~303.48 USD
S3 Storage	                         ~1.27 USD
CloudWatch Monitoring	               ~7 USD
Data Transfer Out	                     0 USD
Tổng chi phí/tháng (ước tính)	     ~312 USD/tháng

6. Lưu ý
Nếu dùng AWS Free Tier, phần S3 (5 GB đầu tiên) & CloudWatch (1 Dashboard) có thể miễn phí → tiết kiệm ~5–7 USD.
Nếu Spot bị reclaim thường xuyên, Core/Task có thể fallback On-Demand, làm chi phí tăng ~20–40% nếu không tối ưu.
Nên dùng Savings Plan/Reserved Instance cho Master Node dài hạn nếu production.
Số giờ chạy Core Nodes có thể giảm nếu workload không 24/7.

C. Architecture Diagrams
(Vẽ sơ đồ: S3 → EMR Master → Spot Core Nodes → Auto Scaling → Monitoring)

D. References
AWS Documentation EMR Spot: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-cluster-spot.html
AWS Pricing Singapore: https://aws.amazon.com/ec2/pricing/on-demand/
AWS CloudWatch: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html


