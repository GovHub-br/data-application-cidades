# Policies, buckets e usuários — como criar à mão

O playbook `install-rustfs.yaml` instala **só o serviço RustFS**, sem nenhum bucket
ou usuário. Isso é de propósito: o mesmo playbook serve para qualquer RustFS. O
que é específico de cada projeto se cria aqui, pelo console web ou pelo `rc`.

Acesse: `http://<IP_DA_VM>:9001`
Login: `rustfs-admin` + a senha impressa no fim do playbook
(ou `cat ~/.clusterlab/credentials/rustfs0/rustfs_secret_key`).

---

## A receita, em 4 passos

Exemplo: um bucket `datalake` e um usuário `app-datalake` que só enxerga ele.

### 1. Criar o bucket

No console, menu de **Buckets** → criar bucket → nome `datalake`.

Deixe Versioning/Object Lock desligados, a menos que você precise.

### 2. Criar a policy

No menu de **Policies** → criar policy → nome `datalake-readwrite` → cole o JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": [
        "arn:aws:s3:::datalake",
        "arn:aws:s3:::datalake/*"
      ]
    }
  ]
}
```

As duas linhas de `Resource` são necessárias e diferentes:

| Linha | Permite |
| --- | --- |
| `arn:aws:s3:::datalake` | mexer no bucket em si (listar o conteúdo) |
| `arn:aws:s3:::datalake/*` | mexer nos arquivos dentro dele |

Só a primeira: o usuário vê o bucket vazio. Só a segunda: ele lê arquivos mas não
consegue listar. **Sempre coloque as duas.**

### 3. Criar o usuário

No menu de **Users** → criar usuário:

- **Access Key** = o "login" (ex.: `app-datalake`)
- **Secret Key** = a "senha" — gere uma longa e aleatória, ex.:

```bash
openssl rand -base64 32
```

### 4. Ligar a policy no usuário

Ainda em **Users**, abra o usuário e atribua a policy `datalake-readwrite`.

Pronto. Esse usuário agora só enxerga o bucket `datalake`.

> Os nomes exatos dos menus podem mudar entre versões do console. Se não achar
> alguma tela, faça pelo `rc` (abaixo) — o resultado é o mesmo.

---

## Variações de policy

**Somente leitura em um bucket:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::datalake",
        "arn:aws:s3:::datalake/*"
      ]
    }
  ]
}
```

**Só uma "pasta" dentro do bucket** (prefixo `logs/`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": ["arn:aws:s3:::datalake/logs/*"]
    }
  ]
}
```

**Acesso total (outro admin)** — use com parcimônia:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["admin:*"] },
    { "Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"] }
  ]
}
```

Regra prática: **um bucket por projeto, uma policy por bucket, um usuário por
aplicação.** Nunca entregue o `rustfs-admin` para uma aplicação.

---

## O mesmo pelo terminal (`rc`)

O playbook já instala o `rc` na VM e configura o alias `rustfs`. Entre por SSH e:

```bash
rc mb rustfs/datalake
rc admin policy create rustfs/ datalake-readwrite ./datalake-readwrite.json
rc admin user add rustfs/ app-datalake 'SENHA_GERADA'
rc admin policy attach rustfs/ datalake-readwrite --user app-datalake
```

Conferir depois:

```bash
rc ls rustfs/
rc admin user list rustfs/
rc admin policy info rustfs/ datalake-readwrite
```

---

## Testar se a permissão está certa

Do seu computador (com o `rc` instalado), com o usuário novo:

```bash
rc alias set teste http://<IP_DA_VM>:9000 app-datalake 'SENHA_GERADA'
rc ls teste/                   # deve mostrar só o datalake
rc ls teste/outro-bucket       # deve dar Access Denied
```

Se o `rc ls teste/` vier vazio, quase sempre falta a linha
`arn:aws:s3:::datalake` (sem o `/*`) na policy.
