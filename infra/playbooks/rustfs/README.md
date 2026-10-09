# RustFS

Sobe o [RustFS](https://github.com/rustfs/rustfs) numa VM Debian 12 — um storage
de objetos compatível com S3, escrito em Rust e com licença Apache 2.0. Serve
como alternativa ao MinIO.

O playbook instala **apenas o serviço**, sem bucket e sem usuário — assim ele serve
para qualquer RustFS. Buckets, policies e usuários são criados depois pelo console
web ou pelo `rc`; o passo a passo está em [`policies/README.md`](policies/README.md).

Este playbook é autocontido: tem o próprio
`inventory.yaml` e o próprio `group_vars/`. Rode sempre de dentro desta pasta.

## Os arquivos

```
infra/playbooks/rustfs/
├── inventory.yaml          # o IP da VM
├── group_vars/rustfs.yaml  # <-- TUDO que você configura fica aqui
├── install-rustfs.yaml     # o playbook
├── policies/README.md      # como criar bucket/policy/usuário à mão
└── README.md
```

A senha do admin **não fica no repositório**. Ela é gravada em
`~/.clusterlab/credentials/<host>/` na sua máquina (ver abaixo).

---

## Como rodar

Antes da primeira vez, troque `<IP_DA_VM>` no `inventory.yaml` pelo IP da VM.

```bash
cd infra/playbooks/rustfs
ansible-playbook -i inventory.yaml install-rustfs.yaml
```

A senha do admin é impressa no final.

Sem `--extra-vars`, sem senha digitada. Pode rodar quantas vezes quiser.

Se preferir conectar como usuário comum em vez de `root`, troque `ansible_user`
no `inventory.yaml` e acrescente `--ask-pass --ask-become-pass`.

---

## As senhas — como funciona

Não existe `.env`. Na primeira execução o Ansible **gera** a senha, grava num
arquivo na sua máquina e mostra no terminal. Nas próximas execuções ele lê o
mesmo arquivo, então a senha nunca muda sozinha.

O arquivo fica **fora do repositório**, de propósito — se ficasse dentro, um
`git add .` distraído mandaria a senha do RustFS para o GitHub:

```
~/.clusterlab/credentials/rustfs0/rustfs_secret_key
```

`rustfs0` é o nome do host no `inventory.yaml`, então cada servidor tem a sua.

Para ver de novo depois:

```bash
cat ~/.clusterlab/credentials/rustfs0/rustfs_secret_key
```

Como a senha tem uma quebra de linha no final, o mais seguro é copiar assim:

```bash
tr -d '\n' < ~/.clusterlab/credentials/rustfs0/rustfs_secret_key | pbcopy
```

Quer definir a senha na mão? É só escrever o valor em `group_vars/rustfs.yaml`
(`rustfs_secret_key: "..."`).

Cuidado: a senha é impressa no terminal. Não rode este playbook em CI sem antes
pôr `no_log: true` na task que imprime, senão ela vai parar no log do job.

---

## Os tipos de "usuário" (é aqui que confunde)

| Nome | Onde vive | Para quê |
| --- | --- | --- |
| `root` | login SSH da VM | você entra na máquina |
| `rustfs` | usuário de sistema da VM | só roda o processo do RustFS, não tem login |
| `rustfs-admin` | **dentro do RustFS** | dono de tudo; loga no console web |
| `app-...` | **dentro do RustFS** | usuários de aplicação, criados à mão depois |

Os dois últimos são contas do RustFS, não do Linux. Só o `rustfs-admin` é criado
pelo playbook. No RustFS o "usuário" e a "senha" se chamam **access key** e
**secret key** (`RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY`).

---

## Sobre o "allow hosts"

RustFS não tem um arquivo de "hosts permitidos" como o `pg_hba.conf` do
PostgreSQL. O equivalente é firewall: o playbook põe o ufw em
`default deny incoming` e libera as portas 9000/9001 só para as redes de
`rustfs_allowed_networks`.

---

## Sobre a versão

As versões ficam fixadas no topo do `install-rustfs.yaml`:

| Variável | Versão | O quê |
| --- | --- | --- |
| `rustfs_version` | `1.0.1` | servidor (build `musl`, estático — não depende da glibc da VM) |
| `rc_version` | `0.1.36` | `rc`, o cliente de linha de comando oficial (equivalente ao `mc`) |

Cada versão é extraída em `/opt/rustfs/<versão>/` e o `/usr/local/bin/rustfs`
é só um symlink. Para atualizar, troque `rustfs_version` e rode o playbook de
novo: ele baixa a nova versão, troca o symlink e reinicia o serviço. A versão
antiga continua em `/opt/rustfs/` para um rollback rápido.

---

## Diferenças em relação ao MinIO

- **Cliente:** em vez do `mc`, o playbook instala o `rc` (`rustfs/cli`), que tem
  quase os mesmos comandos (`rc alias set`, `rc mb`, `rc admin user add`...).
- **Health check:** `/health` e `/health/ready` em vez de `/minio/health/live`.
- **Path-style:** sem `RUSTFS_SERVER_DOMAINS` configurado, o RustFS só aceita
  endereçamento *path-style* (`http://host:9000/bucket/obj`). Nos clientes S3
  (boto3, AWS SDK, Spark, Terraform) ligue `force_path_style` /
  `s3_use_path_style = true`.

---

## Comandos úteis

```bash
ansible rustfs -i inventory.yaml -b -m command -a "systemctl status rustfs"
ansible rustfs -i inventory.yaml -b -m command -a "journalctl -u rustfs -n 50 --no-pager"

# na VM (o alias "rustfs" do rc já vem configurado)
rc ls rustfs/
rc admin user list rustfs/
curl -s http://127.0.0.1:9000/health/ready
```
