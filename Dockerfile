# Apache Iceberg standalone service
FROM openjdk:11-jre-slim

# Set environment variables
ENV ICEBERG_VERSION=1.4.3
ENV HADOOP_VERSION=3.3.6
ENV AWS_SDK_VERSION=1.12.262

# Switch to root user to install packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Create directories
RUN mkdir -p /opt/iceberg/lib /opt/iceberg/conf /tmp/warehouse

# Download Iceberg and Hadoop dependencies
WORKDIR /opt/iceberg/lib

# Download Iceberg core JARs
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/${ICEBERG_VERSION}/iceberg-core-${ICEBERG_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-api/${ICEBERG_VERSION}/iceberg-api-${ICEBERG_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-common/${ICEBERG_VERSION}/iceberg-common-${ICEBERG_VERSION}.jar

# Download Iceberg Hadoop integration
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hadoop/${ICEBERG_VERSION}/iceberg-hadoop-${ICEBERG_VERSION}.jar

# Download Iceberg AWS integration (for S3 support)
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar

# Download Hadoop client libraries
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar

# Download AWS SDK for S3 support
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Create Iceberg configuration for Polaris REST catalog
WORKDIR /opt/iceberg/conf
RUN echo "# Iceberg Configuration with Apache Polaris" > iceberg.properties && \
    echo "catalog-impl=org.apache.iceberg.rest.RESTCatalog" >> iceberg.properties && \
    echo "uri=http://polaris:8181/api/catalog" >> iceberg.properties && \
    echo "warehouse=s3://warehouse/" >> iceberg.properties && \
    echo "credential=polaris-client:polaris-secret" >> iceberg.properties && \
    echo "" >> iceberg.properties && \
    echo "# Fallback Hadoop catalog configuration" >> iceberg.properties && \
    echo "warehouse.hadoop=/tmp/warehouse" >> iceberg.properties && \
    echo "catalog-impl.hadoop=org.apache.iceberg.hadoop.HadoopCatalog" >> iceberg.properties

# Create warehouse directory with proper permissions
RUN mkdir -p /tmp/warehouse && chmod 777 /tmp/warehouse

# Create a Polaris connection test script
RUN echo '#!/bin/bash' > /opt/iceberg/test-polaris.sh && \
    echo 'echo "Testing Polaris connection..."' >> /opt/iceberg/test-polaris.sh && \
    echo 'curl -s -f http://polaris:8181/management/v1/config || echo "Polaris not available"' >> /opt/iceberg/test-polaris.sh && \
    echo 'echo "Polaris connection test completed"' >> /opt/iceberg/test-polaris.sh && \
    chmod +x /opt/iceberg/test-polaris.sh

# Update health check script to include Polaris status
RUN echo '#!/bin/bash' > /opt/iceberg/health-check.sh && \
    echo 'echo "=== Iceberg Health Check ==="' >> /opt/iceberg/health-check.sh && \
    echo 'echo "Warehouse status:"' >> /opt/iceberg/health-check.sh && \
    echo 'ls -la /tmp/warehouse' >> /opt/iceberg/health-check.sh && \
    echo 'echo "Iceberg JARs:"' >> /opt/iceberg/health-check.sh && \
    echo 'ls -la /opt/iceberg/lib/ | wc -l | xargs echo "Available JARs:"' >> /opt/iceberg/health-check.sh && \
    echo 'echo "Polaris connection:"' >> /opt/iceberg/health-check.sh && \
    echo '/opt/iceberg/test-polaris.sh' >> /opt/iceberg/health-check.sh && \
    chmod +x /opt/iceberg/health-check.sh

# Set working directory
WORKDIR /tmp/warehouse

# Expose port for potential web UI or API (if needed)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /opt/iceberg/health-check.sh || exit 1

# Keep container running and provide Iceberg warehouse service
CMD ["tail", "-f", "/dev/null"]
