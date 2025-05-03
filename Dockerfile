# Usar a imagem oficial do Airflow como base
FROM apache/airflow:2.8.1-python3.9

# Mudar para o usuário root temporariamente para instalar pacotes do sistema
USER root

# Atualizar a lista de pacotes e instalar OpenJDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Definir a variável de ambiente JAVA_HOME para OpenJDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Adicionar o diretório bin do Java ao PATH
ENV PATH=$PATH:$JAVA_HOME/bin

# Voltar para o usuário padrão do Airflow
USER airflow

# Copiar o arquivo de requirements e instalar as dependências Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt