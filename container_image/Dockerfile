FROM elastic/elasticsearch:7.17.28

# Switch to root for installations
USER root

# Install system dependencies including curl
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Installs the core Elasticsearch and parquet dependencies used by Apertus indexing.
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install \
    pandas \
    pyarrow \
    "elasticsearch>=7.17,<8.0" \
    numpy \
    psutil \
    requests \
    urllib3 \
    && rm -rf ~/.cache/pip

RUN python3 -m pip install \
    transformers \
    torch \
    tqdm \
    datasets \
    && rm -rf ~/.cache/pip

# Installs the rl-webindex-inspired web ingestion dependencies.
RUN python3 -m pip install \
    protego \
    trafilatura \
    && rm -rf ~/.cache/pip

# Ensure curl is executable
RUN chmod +x /usr/bin/curl

# Create working directory
WORKDIR /capstor/scratch/cscs/inesaltemir

# Switch back to elasticsearch user
USER elasticsearch

# Expose ports
EXPOSE 9200 9300

# Start Elasticsearch
CMD ["/usr/share/elasticsearch/bin/elasticsearch"]