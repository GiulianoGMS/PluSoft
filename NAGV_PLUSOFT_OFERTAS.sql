CREATE OR REPLACE VIEW CONSINCO.NAGV_PLUSOFT_OFERTAS AS

-- Giuliano para Plusoft
-- 29/10/2024

(
-- Desconto de/por

SELECT CODOFERTA, IDPRODUTO, DESCRICAOPRODUTO, 
       TO_CHAR(TO_DATE(DATAINICIO, 'DD/MM/YY'), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') DATAINICIO, 
       TO_CHAR(TO_DATE(DATAFIM, 'DD/MM/YY'), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') DATAFIM, 
       MODALIDADE, QTDMAXIMACOMPRA,
       PERCENTUALDESCONTO, VLRDE, VLRPOR, QTDLEVE, QTDPAGUE, QTDAPARTIRDE, VITRINE, DATAINICIO DATAINICIOF, DATAFIM DATAFIMF

  FROM (
-- Desconto de/por

SELECT DISTINCT A.SEQPROMOCAO||B.NROEMPRESA CODOFERTA,
                B.SEQPRODUTO                IDPRODUTO,
                C.DESCCOMPLETA              DESCRICAOPRODUTO,
                B.DTAINICIOPROM             DATAINICIO,
                B.DTAFIMPROM                DATAFIM,
                'DescontoDePor'             MODALIDADE,
                NULL                        QTDMAXIMACOMPRA,
                NULL                        PERCENTUALDESCONTO,
                E.PRECOVALIDNORMAL          VLRDE,
                E.PRECOVALIDPROMOC          VLRPOR,
                NULL                        QTDLEVE,
                NULL                        QTDPAGUE,
                NULL                        QTDAPARTIRDE,
                'Tabloide'                  VITRINE

  FROM CONSINCO.MRL_PROMOCAO A INNER JOIN CONSINCO.MRL_PROMOCAOITEM B ON (A.SEQPROMOCAO = B.SEQPROMOCAO
                                                                     AND A.NROEMPRESA = B.NROEMPRESA
                                                                     AND A.NROSEGMENTO = B.NROSEGMENTO
                                                                     AND A.CENTRALLOJA = B.CENTRALLOJA
                                                                     AND B.QTDEMBALAGEM = 1)
                               INNER JOIN CONSINCO.MAP_PRODUTO C      ON B.SEQPRODUTO = C.SEQPRODUTO
                               INNER JOIN CONSINCO.MRL_PRODEMPSEG E   ON (B.SEQPRODUTO = E.SEQPRODUTO
                                                                     AND B.QTDEMBALAGEM = E.QTDEMBALAGEM
                                                                     AND B.NROEMPRESA = E.NROEMPRESA
                                                                     AND B.NROSEGMENTO = E.NROSEGMENTO)
 WHERE E.QTDEMBALAGEM = 1
   AND E.NROSEGMENTO <> 5
   AND A.CENTRALLOJA = 'M'
   AND (B.DTAFIMPROM - B.DTAINICIOPROM) <= 15
   AND B.DTAINICIOPROM >= ADD_MONTHS(SYSDATE, -3)

UNION

-- Ativaveis

SELECT DISTINCT A.SEQENCARTE||E.NROEMPRESA  CODOFERTA,
                B.SEQPRODUTO                IDPRODUTO,
                C.DESCCOMPLETA              DESCRICAOPRODUTO,
                A.DTAINICIO                 DATAINICIO,
                A.DTAFIM                    DATAFIM,
                'DescontoDePor'             MODALIDADE,
                NULL                        QTDMAXIMACOMPRA,
                NULL                        PERCENTUALDESCONTO,
                E.PRECOVALIDNORMAL          VLRDE,
                B.PRECOPROMOCIONAL          VLRPOR,
                NULL                        QTDLEVE,
                NULL                        QTDPAGUE,
                NULL                        QTDAPARTIRDE,
                'Ativavel'                  VITRINE

  FROM CONSINCO.MRL_ENCARTE A INNER JOIN CONSINCO.MRL_ENCARTEPRODUTO B ON A.SEQENCARTE = B.SEQENCARTE
                              INNER JOIN CONSINCO.MAP_PRODUTO C        ON B.SEQPRODUTO = C.SEQPRODUTO
                              INNER JOIN CONSINCO.MRL_PRODEMPSEG E     ON (B.SEQPRODUTO = E.SEQPRODUTO AND B.QTDEMBALAGEM = E.QTDEMBALAGEM AND E.NROSEGMENTO <> 5)

 WHERE B.PRECOPROMOCIONAL > 0
   AND E.QTDEMBALAGEM = 1
   AND DESCRICAO LIKE 'MEU NAGUMO%'
   AND A.DTAFIM >= TRUNC(SYSDATE) - 10
   AND E.NROEMPRESA < 301

UNION

-- Meu Nagumo

SELECT DISTINCT A.CODPROMOCAO||E.NROEMPRESA CODOFERTA,
                C.SEQPRODUTO                IDPRODUTO,
                C.DESCCOMPLETA              DESCRICAOPRODUTO,
                A.DTINICIO                  DATAINICIO,
                A.DTFIM                     DATAFIM,
                'DescontoDePor'             MODALIDADE,
                NULL                        QTDMAXIMACOMPRA,
                NULL                        PERCENTUALDESCONTO,
                E.PRECOVALIDNORMAL          VLRDE,
                A.PRECOPPROMOCIONAL         VLRPOR,
                NULL                        QTDLEVE,
                NULL                        QTDPAGUE,
                NULL                        QTDAPARTIRDE,
                'MeuNagumo'                 VITRINE

  FROM CONSINCO.NAGT_REMARCAPROMOCOES A INNER JOIN CONSINCO.MAP_PRODCODIGO B ON TO_CHAR(TO_NUMBER(a.CODIGOPRODUTO)) = B.CODACESSO
                                                                            AND B.TIPCODIGO IN ('B', 'E')
                                        INNER JOIN CONSINCO.MAP_PRODUTO C    ON B.SEQPRODUTO = C.SEQPRODUTO
                                        INNER JOIN CONSINCO.MRL_PRODEMPSEG E ON E.SEQPRODUTO = C.SEQPRODUTO AND E.QTDEMBALAGEM = 1
                                                                            AND E.NROSEGMENTO <> 5 AND E.PRECOBASENORMAL > 0
                                                                            AND E.NROEMPRESA = a.CODLOJA


 WHERE a.DTFIM >= TO_DATE(TRUNC(SYSDATE), 'DD/MM/YYYY')
   AND a.TIPODESCONTO  = 4
   AND a.PROMOCAOLIVRE = 0

UNION ALL

-- Personalizada

SELECT DISTINCT A.SEQENCARTE||E.NROEMPRESA  CODOFERTA,
                C.SEQPRODUTO                IDPRODUTO,
                C.DESCCOMPLETA              DESCRICAOPRODUTO,
                A.DTAINICIO                 DATAINICIO,
                A.DTAFIM                    DATAFIM,
                'DescontoDePor'             MODALIDADE,
                NULL                        QTDMAXIMACOMPRA,
                NULL                        PERCENTUALDESCONTO,
                E.PRECOVALIDNORMAL          VLRDE,
                B.PRECOPROMOCIONAL          VLRPOR,
                NULL                        QTDLEVE,
                NULL                        QTDPAGUE,
                NULL                        QTDAPARTIRDE,
                'Personalizada'             VITRINE

  FROM CONSINCO.MRL_ENCARTE A INNER JOIN CONSINCO.MRL_ENCARTEPRODUTO B ON A.SEQENCARTE = B.SEQENCARTE
                              INNER JOIN CONSINCO.MAP_PRODUTO C        ON B.SEQPRODUTO = C.SEQPRODUTO
                              INNER JOIN CONSINCO.MRL_PRODEMPSEG E     ON B.SEQPRODUTO = E.SEQPRODUTO AND B.QTDEMBALAGEM = E.QTDEMBALAGEM
                           AND E.NROEMPRESA = 14 ---- Solicitação Lucas via e-mail em 15/082024 para enxurgar a quantidade de lojas e deixar apenas uma linha por porduto, pois a personalizada não precisa indicar a loja, por CIpolla

 WHERE E.QTDEMBALAGEM = 1
       AND E.NROSEGMENTO <> 5
       AND DESCRICAO LIKE 'MN PERSONALIZADA%'
       AND A.DTAFIM >= TRUNC(SYSDATE) - 10
       AND E.NROEMPRESA < 500
) )
;
