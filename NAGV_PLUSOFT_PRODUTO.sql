ALTER SESSION SET CURRENT_SCHEMA = CONSINCO;

CREATE OR REPLACE VIEW CONSINCO.NAGV_PLUSOFT_PRODUTO AS

SELECT X.SEQPRODUTO IDPRODUTO,
       X.DESCCOMPLETA TXTDESCRICAOCOMPLETA,
       X.DESCREDUZIDA TXTDESCRICAORESUMIDA,
       DECODE(PESAVEL, 'S', 'KG', 'UN') UNIDADE,
       D.DESCRICAO TXTDEPTO,
       NULL TXTSECAO,
       C.CATEGORIAN1 TXTCATEGORIA,
       C.CATEGORIAN2 TXTGRUPO,
       C.CATEGORIAN3 TXTSUBGRUPO,
       MARCA TXTMARCA,
       NULL TXTFABRICANTE,
       CASE WHEN MARCA = 'NAGUMO' THEN 1 ELSE 0 END FLGMARCAPROPRIA,
       CASE WHEN CATEGORIAN1 = 'SAZONALIDADE' THEN 1 ELSE 0 END FLGPORDUTOSAZONAL,
       CASE WHEN FD.FINALIDADEFAMILIA = 'S' THEN 1 ELSE 0 END FLGSERVICO,
       B.SEQPRODUTO IDMATERIALPAI,
       B.DESCCOMPLETA TXTDESCRICAOMATERIALPAI,
       SEQFORNECEDOR IDFORNECEDOR,
       NOMERAZAO TXTNOMEFORNECEDOR
       
  FROM MAP_PRODUTO X INNER JOIN DIM_CATEGORIA@CONSINCODW C ON C.SEQFAMILIA = X.SEQFAMILIA 
                     INNER JOIN MAP_FAMILIA F ON F.SEQFAMILIA = X.SEQFAMILIA
                      LEFT JOIN MAP_MARCA M ON M.SEQMARCA = F.SEQMARCA
                      LEFT JOIN MAP_FAMFORNEC Z ON Z.SEQFAMILIA = X.SEQFAMILIA AND Z.PRINCIPAL = 'S'
                     INNER JOIN GE_PESSOA G ON G.SEQPESSOA = Z.SEQFORNECEDOR
                      LEFT JOIN MAP_PRODUTO B ON B.SEQPRODUTO = X.SEQPRODUTOBASE 
                     INNER JOIN MAP_FAMDIVISAO FD ON FD.SEQFAMILIA = X.SEQFAMILIA
                      LEFT JOIN MRL_PRODUTOEMPRESA EM ON EM.SEQPRODUTO = X.SEQPRODUTO AND EM.NROEMPRESA = 8
                      LEFT JOIN MRL_DEPARTAMENTO D ON D.NRODEPARTAMENTO = EM.NRODEPARTAMENTO AND D.NROEMPRESA = EM.NROEMPRESA

-- NO DW

CREATE OR REPLACE VIEW CONSINCODW.NAGV_PLUSOFT_PRODUTO AS

SELECT X.SEQPRODUTO IDPRODUTO,
       X.DESCCOMPLETA TXTDESCRICAOCOMPLETA,
       X.DESCREDUZIDA TXTDESCRICAORESUMIDA,
       DECODE(PESAVEL, 'S', 'KG', 'UN') UNIDADE,
       D.DESCRICAO TXTDEPTO,
       NULL TXTSECAO,
       C.CATEGORIAN1 TXTCATEGORIA,
       C.CATEGORIAN2 TXTGRUPO,
       C.CATEGORIAN3 TXTSUBGRUPO,
       M.MARCA TXTMARCA,
       NULL TXTFABRICANTE,
       CASE WHEN M.MARCA = 'NAGUMO' THEN 1 ELSE 0 END FLGMARCAPROPRIA,
       CASE WHEN CATEGORIAN1 = 'SAZONALIDADE' THEN 1 ELSE 0 END FLGPORDUTOSAZONAL,
       CASE WHEN FD.FINALIDADEFAMILIA = 'S' THEN 1 ELSE 0 END FLGSERVICO,
       B.SEQPRODUTO IDMATERIALPAI,
       B.PRODUTO TXTDESCRICAOMATERIALPAI,
       Z.SEQFORNECEDOR IDFORNECEDOR,
       G.NOMERAZAO TXTNOMEFORNECEDOR
       
  FROM MAP_PRODUTO@LINK_C5 X INNER JOIN DIM_CATEGORIA C ON C.SEQFAMILIA = X.SEQFAMILIA 
                     INNER JOIN MAP_FAMILIA@LINK_C5 F ON F.SEQFAMILIA = X.SEQFAMILIA
                      LEFT JOIN MAP_MARCA@LINK_C5 M ON M.SEQMARCA = F.SEQMARCA
                      LEFT JOIN MAP_FAMFORNEC@LINK_C5 Z ON Z.SEQFAMILIA = X.SEQFAMILIA AND Z.PRINCIPAL = 'S'
                     INNER JOIN GE_PESSOA@LINK_C5 G ON G.SEQPESSOA = Z.SEQFORNECEDOR
                      LEFT JOIN DIM_PRODUTO B ON B.SEQPRODUTO = X.SEQPRODUTOBASE 
                     INNER JOIN MAP_FAMDIVISAO@LINK_C5 FD ON FD.SEQFAMILIA = X.SEQFAMILIA
                      LEFT JOIN MRL_PRODUTOEMPRESA@LINK_C5 EM ON EM.SEQPRODUTO = X.SEQPRODUTO AND EM.NROEMPRESA = 8
                      LEFT JOIN MRL_DEPARTAMENTO@LINK_C5 D ON D.NRODEPARTAMENTO = EM.NRODEPARTAMENTO AND D.NROEMPRESA = EM.NROEMPRESA


