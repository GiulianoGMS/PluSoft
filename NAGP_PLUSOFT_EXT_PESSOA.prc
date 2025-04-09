CREATE OR REPLACE PROCEDURE NAGP_PLUSOFT_EXT_PESSOA IS

    v_file UTL_FILE.file_type;
    v_line VARCHAR2(32767);
    v_Targetcharset varchar2(40 BYTE);
    v_Dbcharset varchar2(40 BYTE);
    v_Cabecalho VARCHAR2(4000);
    v_LineConteudo VARCHAR2(4000);
    v_Periodo VARCHAR2(10);
    v_buffer CLOB;
    v_chunk_size CONSTANT PLS_INTEGER := 32000; -- Ajuste conforme necessário

BEGIN
  
    SELECT REPLACE(TO_CHAR(SYSDATE-1, 'DD/MM'),'/','_') 
      INTO v_Periodo
      FROM DUAL;
    -- Abre o arquivo para escrita
    v_file := UTL_FILE.fopen('PLUSOFT', 'Ext_Plusoft_Pessoa_'||v_Periodo||'.csv', 'w', 32767);

    -- Pega o nome das colunas para inserir no cabecalho pq tenho preguica
    SELECT LISTAGG(COLUMN_NAME,';') WITHIN GROUP (ORDER BY COLUMN_ID)
      INTO v_Cabecalho
      FROM ALL_TAB_COLUMNS A
     WHERE A.table_name = 'NAGV_PLUSOFT_PESSOA'
       AND COLUMN_NAME != 'DATA';
    -- Nao utiliza pq nao deu certo na variavel   
       /*    
    SELECT 'vda.'||LISTAGG(COLUMN_NAME,'||'||;||'||vda.') WITHIN GROUP (ORDER BY COLUMN_ID)
      INTO v_LineConteudo
      FROM ALL_TAB_COLUMNS A
     WHERE A.table_name = 'NAGV_PLUSOFT_OFERTAS'
       AND COLUMN_NAME != 'DATA';
       */
    -- Escreve o cabe¿alho do CSV
    UTL_FILE.put_line(v_file, v_Cabecalho);

    -- Executa a query e escreve os resultados

      FOR vda IN ( SELECT IDPESSOA,
                          TXTNOMECOMPLETO,
                          TXTESTADOCIVIL,
                          TXTSEXO,
                          DATNASCIMENTO,
                          DATULTCOMPRA,
                          DATATUALIZACAOCADASTRO,
                          DATCADASTRO,
                          NUMCEL1,
                          NUMCEL2,
                          NUMTELFIXO,
                          NUMTELCOMERCIAL,
                          TXTEMAIL,
                          TXTENDERECO,
                          TXTBAIRRO,
                          TXTCIDADE,
                          ESTADO                                   
                    FROM NAGV_PLUSOFT_PESSOA_ext@CONSINCODW X
                   WHERE 1=1 
                     AND TRUNC(DATATUALIZACAOCADASTRO) = TRUNC(SYSDATE) -1) 

      LOOP

      v_line := vda.IDPESSOA||';'||vda.TXTNOMECOMPLETO||';'||vda.TXTESTADOCIVIL||';'||vda.TXTSEXO||';'||vda.DATNASCIMENTO||';'||vda.DATULTCOMPRA||';'||vda.DATATUALIZACAOCADASTRO||';'||vda.DATCADASTRO||';'||vda.NUMCEL1||';'||vda.NUMCEL2||';'||vda.NUMTELFIXO||';'||vda.NUMTELCOMERCIAL||';'||vda.TXTEMAIL||';'||vda.TXTENDERECO||';'||vda.TXTBAIRRO||';'||vda.TXTCIDADE||';'||vda.ESTADO;
      v_buffer := v_buffer || v_line || CHR(10); -- Adiciona nova linha ao buffer        
        
        IF LENGTH(v_buffer) > v_chunk_size THEN
            UTL_FILE.put_line(v_file, v_buffer); -- Escreve o buffer no arquivo
            v_buffer := ''; -- Limpe o buffer
            
        END IF;
        
    END LOOP;
    
    -- Grava o restante do buffer no final (burro esqueceu)
    IF v_buffer IS NOT NULL THEN
        UTL_FILE.put_line(v_file, v_buffer);
        v_buffer := '';
    END IF;
    
    -- Fecha o arquivo
    UTL_FILE.fclose(v_file);

COMMIT;
EXCEPTION

    WHEN OTHERS THEN
        IF UTL_FILE.is_open(v_file) THEN
            UTL_FILE.fclose(v_file);
        END IF;
        RAISE;

END;
