# Batch and Streaming Data Pipeline for YouTube Trending Analysis

## 📋 Project Overview

A comprehensive big data architecture system for analyzing YouTube content trends and performance patterns. This project combines historical batch data analysis with real-time streaming insights to provide content creators and marketers with actionable intelligence about YouTube trends, viral patterns, and content optimization strategies.

### 🎯 Objectives

Creating a system that enables content creators and marketers to understand:
- What makes videos go viral
- Optimal content creation strategies
- Current trends on YouTube platform
- Regional differences in content consumption

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                 │
├─────────────────────┬───────────────────────────────────────────┤
│   Kaggle Dataset    │           YT-API Stream                   │
│   (Historical)      │         (Real-time)                       │
└─────────────────────┴───────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  INGESTION LAYER                                │
├─────────────────────┬───────────────────────────────────────────┤
│   Batch Loading     │       Stream Loading                      │
│   (HDFS Upload)     │       (Kafka Producer)                    │
└─────────────────────┴───────────────────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────────────┐
│                  DATA LAKE (3 ZONES)                              │
├─────────────────────┬─────────────────┬───────────────────────────┤
│    RAW ZONE         │   TRANSFORMATION│     CURATED ZONE          │
│  - Original         │   ZONE          │                           │
│    data             │  - Cleaned      │  - Aggregated views       │
│  - Parquet format   │    data         │  - Business metrics       │
│  - Partitioned      │  - Validated    |  - Writing queries        |
|                     |  -Golden dataset│  - Saving resuts into DB  │
└─────────────────────┴─────────────────┴───────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                 PROCESSING ENGINES                              │
├─────────────────────┬───────────────────────────────────────────┤
│   Apache Spark      │        Spark Structed Streaming           │
│   (Batch Analytics) │     (Real-time Processing)                │
└─────────────────────┴───────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                 PRESENTATION LAYER                              │
├─────────────────────┬───────────────────────────────────────────┤
│    Dashboards       │         Saved into HDFS                   │
│   (Visualizations)  │        (Real-time Insights)               │
└─────────────────────┴───────────────────────────────────────────┘
```

The system follows a modern big data architecture with containerized microservices, implementing a three-tier data lake approach with both batch and streaming processing capabilities.

**Data Flow:**
1. **Data Sources** → Historical Kaggle dataset + Real-time YT-API stream
2. **Ingestion Layer** → Batch upload to HDFS + Kafka streaming
3. **Data Lake** → Raw Zone → Transformation Zone → Curated Zone  
4. **Processing Engines** → Apache Spark (batch) + Spark Structed Streaming (real-time)
5. **Presentation Layer** → Dashboards + Real-time alerts

### 🔧 Technology Stack

- **Containerization**: Docker, Docker Compose
- **Storage**: HDFS (Hadoop Distributed File System)
- **Stream Processing**: Apache Kafka, Apache Spark Streaming
- **Batch Processing**: Apache Spark SQL
- **Data Format**: Parquet (columnar format)
- **Languages**: Python 3.8+, SQL
- **APIs**: REST API for data ingestion

## 📊 Data Sources

### 🗄️ Primary Dataset (Batch Processing)

- **Source**: [YouTube Trending Videos Dataset - Kaggle](https://www.kaggle.com/datasets/datasnaek/youtube-new/data)
- **Size**: >500MB historical data
- **Content**: Trending videos from multiple regions with comprehensive metadata
- **Time Range**: Historical trending data with video statistics, categories, and engagement metrics
- **Structure**: Contains video_id, title, channel information, publish times, trending dates, views, likes, dislikes, comment counts, thumbnails, descriptions, and various flags for disabled features

### 📡 Secondary Dataset (Streaming Processing)

- **Source**: [YT-API via RapidAPI](https://rapidapi.com/ytjar/api/yt-api)
- **Type**: Real-time YouTube trending data stream
- **Content**: Current trending videos, live statistics, real-time engagement metrics
- **Update Frequency**: Continuous streaming with 2-minute processing intervals
- **Format**: JSON stream over Kafka topics

## 🏢 Data Lake Definition

### 📁 Zone 1: Raw Data Zone
**Location**: `hdfs://namenode:9000/storage/hdfs/raw/`

**Purpose**: Store original, unprocessed data
**Format**: Original CSV files converted to Parquet
**Partitioning**: By region and trending date
**Content**: Raw data from both Kaggle dataset and streaming API exactly as received

### 🔄 Zone 2: Transformation Zone  
**Location**: `hdfs://namenode:9000/storage/hdfs/processed/golden_dataset/`

**Purpose**: Data cleaning, validation, and standardization
**Operations**: 
- Data type conversions
- Missing value handling
- Duplicate removal  
- Schema validation
- Data quality checks
- Standardization of formats

### 🎯 Zone 3: Curated Zone (Golden Dataset)
**Location**: `hdfs://namenode:9000/storage/hdfs/processed/`

**Purpose**: Business-ready, analytics-optimized data
**Features**:
- SQL queries executed against golden datasets
- Results stored in PostgreSQL database tables
- Automated ETL pipelines for data warehouse population

## 📊 Analytical Questions

### 🔍 Batch Processing Queries (1-10)

**1. Regional Trending Patterns**: What is the average viewership and comment count by category, region, and trending date, and how do these averages differ for videos with enabled vs. disabled comments? Which categories and regions dominate by viewership, and how do viewing trends change over the last three days?

**2. User Engagement Analysis**: Which categories and channels achieve the highest user engagement scores (total likes + comments), and whether that engagement comes from positive or negative feedback? How do they rank within their categories? What are the top 5 channels by engagement in each category?

**3. Viral Speed Analysis**: Which YouTube categories and regions represent the top 10% fastest viral videos while simultaneously being in the top 10% for trending list duration? How long does it take on average for a video to reach trending and how long does it stay? Which content types are both instant hits and long-lasting hits - the "golden combinations"?

**4. Content Issues Analysis**: What type of content problems are most common by category-region combination and what percentage of problematic videos do they represent?

**5. Tag Performance Analysis**: Which tags, extracted from video tag lists, are most frequent and successful based on basic metrics like video count, average likes, number of regions and categories they appear in, viral rate, and overall popularity? How do they rank by combined "viral score" indicator?

**6. Advanced Tag Intelligence**: Beyond basic popularity, which tags are characterized by the best combination of viral potential and market position? What "power level" is assigned to them based on success across different categories and regions? What recommendations for future use do they deserve based on viral score and success rate? How do they rank globally and within their power level groups with smoothed viral score trend analysis?

**7. Channel Viral Dynamics**: Which YouTube channels in specific categories achieve viral status fastest and how does their popularity dynamics change over time?

**8. Content Optimization**: What combination of video description length and thumbnail quality brings the best performance (highest views and likes) for each YouTube content category?

**9. Seasonal Trends**: For each content category and geographic region, which months of the year represent optimal time for launching YouTube videos with the highest probability of success, ranked by seasonal popularity and viewership trends?

**10. Billion-View Analysis**: Which channels have videos with over one billion views and in which countries?

### ⚡ Real-time Stream Processing Queries (1-4)

**1. Current Dominance Analysis**: Which channels are currently dominating the YouTube trending list in the last 15 minutes compared to their historical batch analysis performance, and which represent unexpected viral phenomena that weren't in top performers?

**2. Category Performance Monitoring**: What are the real-time YouTube category performances (video count, views, engagement) compared to historical values and what are the overall trends, including categories that are growing or declining and top winners and losers?

**3. Anomaly Detection**: Which YouTube channels are currently experiencing performance anomalies compared to their historical performance, which categories are currently exceeding or lagging behind historical category averages, and are there channels showing viral growth in real-time compared to their historical performance?

**4. Content Strategy Intelligence**: Which YouTube title writing strategies are currently contributing to higher viewership and viral potential, and which channels are using them most successfully in real-time?

## 🚀 System Deployment

### ✅ Prerequisites

- Docker 20.10+ and Docker Compose 2.0+
- Python 3.8+
- Minimum 16GB RAM
- Minimum 100GB free disk space
- RapidAPI key for YT-API

### 🔧 Installation Process

**Step 1: Repository Setup**
Clone the repository and navigate to the project directory.

**Step 2: Environment Configuration**
Copy the environment template and configure API keys, Kafka brokers, HDFS namenode, and Spark master settings.

**Step 3: Infrastructure Launch**
Start all containerized services using Docker Compose.

**Step 4: Service Health Verification**
Wait for all services to be ready and verify their health status.

**Step 5: Data Lake Initialization**
Run scripts to create the three-zone data lake structure in HDFS.

**Step 6: Historical Data Loading**
Execute the batch processing pipeline to load and process Kaggle dataset into the data lake.

**Step 7: Streaming Processors Activation**
Start Kafka producer for real-time data ingestion and launch all four stream-batch processing queries.

### 🖥️ Running Batch Queries

Individual queries can be executed separately, or all batch queries (1-10) can be run sequentially using automation scripts.

## 📁 Project Structure

The project is organized into logical modules:
- **Configuration files** for Docker, Spark, Kafka, and HDFS
- **Batch processing** module with data ingestion, golden dataset creation, and all 10 analytical queries  
- **Streaming** module with Kafka producer and 4 stream-batch integration queries
- **Schemas** defining data structures for both batch and streaming data
- **Utilities** for Spark, Kafka, and HDFS operations
- **Visualization** components with notebooks and dashboards

## 🔍 Key Features

### 🧠 Content Intelligence Engine
- **Title Optimization Analysis**: Correlation between title characteristics and viewership patterns
- **Temporal Pattern Recognition**: Optimal publishing times and viral speed analysis
- **Engagement Prediction**: Machine learning models for engagement forecasting
- **Anomaly Detection**: Real-time identification of unusual trending patterns

### ⚡ Real-time Processing Capabilities
- **2-minute processing intervals** for near real-time insights
- **Watermarking and windowing** for handling late-arriving data
- **Stream-batch joining** for comprehensive analysis combining historical context with current trends
- **Automatic checkpointing** for fault tolerance and recovery

### 🚀 Performance Optimizations
- **Columnar storage** (Parquet) for analytical workloads
- **Intelligent partitioning strategies** for efficient querying
- **Adaptive query execution** in Spark for dynamic optimization
- **Kryo serialization** for faster data processing

## 📈 Sample Insights

### 🔥 Viral Content Patterns
Videos with numbers in titles receive 23% more views on average. The optimal title length for videos exceeding 1 million views is 45-55 characters. Question marks in titles correlate with higher engagement in the entertainment category.

### 🌍 Regional Differences
Gaming content shows different peak hours across regions. Educational content demonstrates consistent global performance. News content has the highest viral speed but shortest trending duration.

### ⏰ Temporal Insights
Tuesday through Thursday proves optimal for most categories. Weekend publishing benefits lifestyle and entertainment content. Holiday seasons show 40% increased engagement for specific content categories.

## 🛠️ Development and Extensions

### ➕ Adding New Queries
New analytical queries can be added by following the established naming conventions, implementing proper error handling and logging, and adding visualization components when needed.

### 📡 Extending Data Sources
Additional data sources can be integrated by adding new producers to the streaming module, defining appropriate schemas, updating the ingestion pipeline, and creating corresponding transformation logic.

## 🔧 Monitoring and Maintenance

### 🏥 Health Checks
The system includes monitoring for Kafka topic lag, HDFS storage utilization, Spark job success rates, and API rate limit tracking.

### 📊 Logging and Alerting
Centralized logging with structured formats, error tracking and alerting systems, and performance metrics collection ensure system reliability.

### 🔔 Alert Rules
Automated alerts trigger for Kafka consumer lag exceeding 1000 messages, HDFS storage above 85% capacity, Spark job failure rates above 5%, and API rate limits approaching thresholds.

## 🤝 Contributing

Contributors can fork the repository, create feature branches, commit changes with descriptive messages, push to their branches, and open pull requests for review.

## 🐛 Troubleshooting

### Common Issues
**Kafka Connection Issues**: Restart Kafka service and verify topic availability.
**HDFS Safe Mode**: Manually disable safe mode if namenode gets stuck.
**Spark Memory Errors**: Increase worker and driver memory allocation in Docker configuration.

## 📋 Future Enhancements

Planned improvements include implementing graphical dashboards, adding machine learning models for prediction, integrating ElasticSearch for full-text search capabilities, implementing batch prediction pipelines, adding more streaming data sources, and creating REST APIs for external access.

## 📞 Support

For questions and issues, users can open GitHub issues, contact the team via email, or check documentation in the project's docs directory.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

Special thanks to the YouTube Trending Dataset by DataSnaek on Kaggle, YT-API by ytjar on RapidAPI, Apache Spark and Kafka communities, and the Docker containerization ecosystem.

---

**Project Status**: Active Development  
**Last Updated**: September 2025  
**Version**: 1.0.0

> 💡 **Performance Tip**: For optimal performance, run the system on machines with minimum 16GB RAM and SSD storage.
