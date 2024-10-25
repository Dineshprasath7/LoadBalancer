# SHIELD: Strategic Host Intrusion Elimination and Load Distribution

## Overview

SHIELD is a robust Python-based load balancer designed to intelligently distribute incoming client requests across multiple backend servers while providing an intrusion detection mechanism to analyze packet data for potential threats. This project enhances the availability and security of web services in distributed systems and cloud computing environments.
---
## Key Features

- **Dynamic Load Balancing**: Supports various load balancing algorithms including:
  - Least Connections
  - Weighted Round Robin
  - IP Hashing
  - Round Robin
  - Dynamic balancing based on server performance metrics (CPU and memory usage)

- **Intrusion Detection**: Utilizes the `PacketAnalyzer` class to examine incoming packets for known attack signatures, including:
  - SQL Injection
  - Cross-Site Scripting (XSS)
  - Command Injections
  - Malicious File Uploads

- **Rate Limiting**: Implements a feature to restrict the number of requests from a single IP address, mitigating the risk of Denial-of-Service (DoS) attacks.

- **Health Checks**: Regularly checks the status and responsiveness of backend servers, allowing dynamic adjustment of request routing based on server load.

- **Real-time Notifications**: Equipped with a GUI notification system to alert administrators about detected anomalies or security threats.


---



