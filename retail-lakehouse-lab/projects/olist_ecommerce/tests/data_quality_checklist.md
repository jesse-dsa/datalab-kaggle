# data_quality_checklist

Checklist mínimo para evolução do projeto:

- validar unicidade de chaves primárias por entidade
- verificar campos críticos nulos em pedidos, clientes, pagamentos e itens
- confirmar consistência de tipos para datas, valores monetários e identificadores
- medir duplicidade após ingestão e após tratamento
- documentar regras de negócio aplicadas em Silver
- validar coerência das métricas finais expostas em Gold
