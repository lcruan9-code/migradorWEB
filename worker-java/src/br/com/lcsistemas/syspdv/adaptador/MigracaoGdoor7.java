package br.com.lcsistemas.syspdv.adaptador;

import java.sql.Connection;

/**
 * Estratégia de migração para Gdoor 7.x — ESTRUTURA FUTURA (não implementada).
 *
 * <p>Esta classe existe para demonstrar a extensibilidade da arquitetura.
 * Quando o Gdoor 7.x precisar ser suportado:
 * <ol>
 *   <li>Implemente a lógica dentro do método {@link #executar(Connection)}.</li>
 *   <li>Adicione esta classe à lista da {@link FabricaMigracao} para as versões desejadas.</li>
 *   <li>Nenhum outro arquivo precisa ser alterado.</li>
 * </ol>
 *
 * <p><b>RESTRIÇÃO:</b> Não implementar lógica ainda. Apenas estrutura.
 *
 * <p>Preparação para extensão futura (Fase 8 — não implementar agora):
 * <ul>
 *   <li>Detector de estrutura de banco (RDB$RELATIONS)</li>
 *   <li>Mapeamento por JSON</li>
 *   <li>Scripts híbridos (código + SQL)</li>
 *   <li>Auto-start de serviço Firebird</li>
 * </ul>
 *
 * @see MigracaoGdoor5
 * @see MigracaoGdoor6
 * @see FabricaMigracao
 */
public class MigracaoGdoor7 implements AdaptadorMigracao {

    private static final String NOME = "MigracaoGdoor7";

    @Override
    public String getNome() {
        return NOME;
    }

    /**
     * Execução da migração Gdoor 7.x.
     *
     * <p><b>TODO (futuro):</b> Implementar quando o Gdoor 7.x for suportado.
     *
     * @param conn Conexão JDBC de origem (Firebird).
     * @throws UnsupportedOperationException sempre — estratégia não implementada.
     */
    @Override
    public void executar(Connection conn) throws Exception {
        throw new UnsupportedOperationException(
            "[" + NOME + "] Migração Gdoor 7.x ainda não implementada. "
            + "Estrutura criada para evolução futura."
        );
    }
}
