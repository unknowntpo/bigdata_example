# Claude Project Memory - Twitter Analytics System

## Project Context
- **Project Type**: Educational big data analytics system
- **User Goal**: Learn Hadoop ecosystem through hands-on Twitter analytics implementation
- **Approach**: Step-by-step guided development where I implement infrastructure and guide user through application coding

## Current Status
**Phase 1: Infrastructure Setup - IN PROGRESS**

### Completed Tasks ✅
1. **Docker Compose Setup**: Created comprehensive docker-compose.yml with:
   - Hadoop cluster (NameNode, DataNode, ResourceManager, NodeManager, HistoryServer)
   - Network configuration and volume management
   - Port mappings for web UIs (9870, 8088, 8188)

2. **Hadoop Configuration**: Created configuration files in `hadoop-config/`:
   - `core-site.xml`: Core Hadoop settings, HDFS URI
   - `hdfs-site.xml`: HDFS configuration, replication, permissions
   - `yarn-site.xml`: YARN resource management configuration  
   - `mapred-site.xml`: MapReduce job configuration

### Next Steps 🔄
3. **Spark Integration**: Add Spark Master/Worker to docker-compose
4. **Hive + PostgreSQL**: Configure Hive metastore with PostgreSQL
5. **Kafka Setup**: Add Kafka for streaming data ingestion
6. **Init Scripts**: Create startup and data preparation scripts

## Technical Decisions Made
- Using `bde2020/hadoop-*` Docker images for Hadoop ecosystem
- Single DataNode setup (suitable for development/learning)
- Disabled HDFS permissions for easier development
- Configured for resource constraints (16GB RAM, 8 cores for NodeManager)

## Key Files Created
- `docker-compose.yml`: Main infrastructure orchestration
- `hadoop-config/*.xml`: Hadoop cluster configuration
- `plan.md`: Detailed project roadmap
- `claude.md`: This memory file

## User Learning Path
1. **Infrastructure Understanding**: Docker Compose, Hadoop components
2. **Application Development**: Java/Gradle with big data libraries  
3. **Data Pipeline**: Kafka → HDFS → Spark/Hive processing
4. **Analytics Implementation**: Celebrity ranking, trend analysis

## Commands for User
```bash
# Start Hadoop cluster
docker-compose up -d

# Check Hadoop status
docker-compose ps

# Access Hadoop UIs:
# - NameNode: http://localhost:9870
# - ResourceManager: http://localhost:8088  
# - HistoryServer: http://localhost:8188
```
- for app logic , guide me to impl , for model creation , you should do it for me