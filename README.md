# Fabric Warehouse Data Generator

Gerador de dados sintéticos para Microsoft Fabric Warehouse que insere dados continuamente em três tabelas relacionadas: `customers`, `orders` e `payments`.

## Características

- Geração de dados realistas usando a biblioteca Faker
- Inserção em lote (batch) para alta performance
- Autenticação Azure com múltiplos métodos
- Inserção contínua configurável (padrão: 10.000 linhas a cada 5 segundos)
- Suporte a três tabelas relacionadas
- Interface colorida no terminal
- Tratamento robusto de erros

## Estrutura das Tabelas

### customers
- `ID` (INT, PRIMARY KEY)
- `FIRST_NAME` (NVARCHAR)
- `LAST_NAME` (NVARCHAR)

### orders
- `ID` (INT, PRIMARY KEY)
- `USER_ID` (INT, referência ao customer)
- `ORDER_DATE` (DATETIME)
- `STATUS` (NVARCHAR)

### payments
- `ID` (INT, PRIMARY KEY)
- `ORDERID` (INT, referência ao order)
- `PAYMENTMETHOD` (NVARCHAR)
- `STATUS` (NVARCHAR)
- `AMOUNT` (DECIMAL)
- `CREATED` (DATETIME)

## Pré-requisitos

1. **Python 3.9+** instalado
2. **ODBC Driver 18 for SQL Server** instalado
   - Windows: [Download aqui](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
   - Linux: Siga as instruções da Microsoft
3. **Microsoft Fabric Warehouse** configurado e acessível
4. **Autenticação Azure** configurada (uma das opções):
   - Azure CLI instalado e logado (`az login`)
   - Service Principal com permissões
   - Conta com acesso ao Fabric Warehouse

## Instalação

1. Clone ou baixe este repositório

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

3. Configure as credenciais:
```bash
# Copie o arquivo de exemplo
cp .env.example .env

# Edite o arquivo .env com suas credenciais
# Use seu editor preferido (notepad, vim, nano, etc.)
```

## Configuração

Edite o arquivo `.env` com suas configurações:

```env
# Endpoint do seu Fabric Warehouse (sem https://)
FABRIC_SERVER=seu-workspace.datawarehouse.fabric.microsoft.com

# Nome do database
FABRIC_DATABASE=seu_database

# Método de autenticação: CLI, INTERACTIVE ou SERVICE_PRINCIPAL
AUTH_METHOD=CLI

# Para SERVICE_PRINCIPAL, configure também:
# AZURE_CLIENT_ID=...
# AZURE_CLIENT_SECRET=...
# AZURE_TENANT_ID=...

# Configurações de geração
BATCH_SIZE=10000
BATCH_INTERVAL=5
```

### Métodos de Autenticação

#### 1. Azure CLI (Recomendado para desenvolvimento)
```bash
AUTH_METHOD=CLI
```
Execute `az login` antes de rodar o script.

#### 2. Interactive Browser
```bash
AUTH_METHOD=INTERACTIVE
```
Uma janela do browser abrirá para autenticação.

#### 3. Service Principal (Recomendado para produção)
```bash
AUTH_METHOD=SERVICE_PRINCIPAL
AZURE_CLIENT_ID=seu-client-id
AZURE_CLIENT_SECRET=seu-client-secret
AZURE_TENANT_ID=seu-tenant-id
```

## Uso

Execute o gerador:

```bash
python generator.py
```

O script irá:
1. Conectar ao Fabric Warehouse
2. Criar as tabelas se não existirem
3. Iniciar a geração contínua de dados
4. Inserir lotes de dados no intervalo configurado
5. Mostrar estatísticas em tempo real

Para parar, pressione `Ctrl+C`.

### Exemplo de Saída

```
============================================================
Fabric Warehouse Data Generator
============================================================
Batch Size: 10,000 rows per table
Interval: 5 seconds
============================================================

Using Azure CLI authentication...
✓ Successfully connected to Fabric Warehouse
✓ Tables verified/created successfully

Starting data generation... (Press Ctrl+C to stop)

[Batch #1] Starting at 2025-10-14 10:30:00
  → Generating 10,000 customers...
  → Generating 10,000 orders...
  → Generating 10,000 payments...
  → Inserting customers...
  → Inserting orders...
  → Inserting payments...
  ✓ Batch #1 completed in 2.34s (12,820 rows/sec)
  Total inserted: 10,000 customers, 10,000 orders, 10,000 payments

[Batch #2] Starting at 2025-10-14 10:30:07
...
```

## Performance

- **Batch Size**: 10.000 linhas por tabela = 30.000 linhas totais por lote
- **Intervalo**: 5 segundos entre lotes
- **Taxa estimada**: ~6.000 linhas/segundo (360.000 linhas/minuto)
- **Volume diário**: ~51.8 milhões de linhas (24h contínuo)

A performance real depende de:
- Latência de rede
- Capacidade do Fabric Warehouse
- Recursos da máquina local

## Customização

### Ajustar volume de dados

Edite no `.env`:
```env
BATCH_SIZE=5000    # Reduzir para menos dados
BATCH_INTERVAL=10  # Aumentar intervalo
```

### Modificar dados gerados

Edite as funções em `generator.py`:
- `generate_customers()`: Customizar estrutura de clientes
- `generate_orders()`: Modificar status, datas, etc.
- `generate_payments()`: Alterar métodos de pagamento, valores, etc.

### Adicionar novas colunas

1. Modifique a função `create_tables_if_not_exist()` para adicionar colunas
2. Atualize as funções `generate_*()` correspondentes
3. Ajuste as funções `batch_insert_*()` se necessário

## Troubleshooting

### Erro de autenticação
```
Authentication error: ...
```
- Verifique se executou `az login` (para CLI)
- Confirme as credenciais no `.env`
- Verifique permissões no Fabric Warehouse

### Erro de conexão timeout
```
Connection timeout...
```
- Verifique o FABRIC_SERVER (sem https://)
- Confirme conectividade de rede
- Verifique firewall/proxy

### ODBC Driver não encontrado
```
Driver not found...
```
- Instale o ODBC Driver 18 for SQL Server
- No Windows, pode ser necessário reiniciar após instalação

### Erro ao criar tabelas
```
Permission denied...
```
- Verifique permissões de escrita no database
- Confirme que o database existe
- Crie as tabelas manualmente se necessário

## Estrutura do Projeto

```
fabric_data_generator/
├── generator.py          # Script principal
├── requirements.txt      # Dependências Python
├── .env.example         # Exemplo de configuração
├── .env                 # Suas credenciais (não commitar!)
└── README.md           # Esta documentação
```

## Licença

Este projeto é fornecido como exemplo educacional.

## Contribuições

Sugestões e melhorias são bem-vindas!
