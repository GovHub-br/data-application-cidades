FROM apache/airflow:3.3.1-python3.11

# acraiz.icpbrasil.gov.br não envia toda a cadeia de certificados
# por isso a conexão TLS é rejeitada pelo curl / openssl

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         libgtk2.0-dev \
         libgdal-dev \
         unixodbc-dev \
         libpq-dev \
         vim \
         unzip \
         git \
         curl \
  && curl -O http://acraiz.icpbrasil.gov.br/credenciadas/CertificadosAC-ICP-Brasil/ACcompactado.zip \
  && unzip -o ACcompactado.zip -d /usr/local/share/ca-certificates/ \
  && update-ca-certificates \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base \
  && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
  && sed -i 's/^# pt_BR.UTF-8 UTF-8$/pt_BR.UTF-8 UTF-8/g' /etc/locale.gen \
  && locale-gen en_US.UTF-8 pt_BR.UTF-8 \
  && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

# Para rodar o airflow só precisamos instalar as dependências visto que o código
# sempre será sincronizado via git sync ou via volumes localmente
# Deve ser instalado como root para tornar-se somente leitura
COPY requirements.txt .
RUN uv pip install --no-cache-dir -r requirements.txt

USER airflow
