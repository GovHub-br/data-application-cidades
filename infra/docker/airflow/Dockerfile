FROM apache/airflow:3.3.2-python3.11

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
         # mdbtools: binário do sistema (não é lib Python) que o raw_para_staging.py usa
         # para ler os .mdb/.accdb do lake — sem ele a conversão dessa família falha em
         # bloco no mdb_disponivel().
         mdbtools \
  && sed -i 's,^\(MinProtocol[ ]*=\).*,\1'TLSv1.0',g' /etc/ssl/openssl.cnf \
  && sed -i 's,^\(CipherString[ ]*=\).*,\1'DEFAULT@SECLEVEL=1',g' /etc/ssl/openssl.cnf \
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

USER airflow
WORKDIR ${AIRFLOW_HOME}

# Senhas do SimpleAuthManager, o login padrao do Airflow 3 (o FAB e o
# `airflow users create` sairam do core). Sem o arquivo, o standalone sorteia
# uma senha nova a cada container recriado.
COPY --chown=airflow:0 infra/airflow/simple_auth_manager_passwords.json ./simple_auth_manager_passwords.json

# Para rodar o airflow só precisamos instalar as dependências visto que o código
# sempre será sincronizado via git sync ou via volumes localmente
COPY requirements.txt .
# As duas checagens depois do install fazem o build reprovar em vez de gerar uma
# imagem quebrada: o pip rebaixava o Airflow em silêncio quando a imagem base
# mudava de versão (AIRFLOW_VERSION vem da imagem oficial), e só avisava, sem
# falhar, quando deixava pacotes da base incompatíveis.
RUN pip install --no-cache-dir -r requirements.txt \
  && python -c "import airflow, os, sys; esperado = os.environ['AIRFLOW_VERSION']; print(f'Airflow {airflow.__version__} (base {esperado})'); sys.exit(airflow.__version__ != esperado)" \
  && pip check
