package br.com.lcsistemas.syspdv.adaptador;

import java.sql.Connection;

/**
 * Estratégia de migração para Gdoor 6.x — ESTRUTURA FUTURA (não implementada).
 *
 * <p>Esta classe existe para demonstrar a extensibilidade da arquitetura.
 * Quando o Gdoor 6.x precisar ser suportado:
 * <ol>
 *   <li>Implemente a lógica dentro do método {@link #executar(Connection)}.</li>
 *   <li>Adicione esta classe à lista da {@link FabricaMigracao} para as versões desejadas.</li>
 *   <li>Nenhum outro arquivo precisa ser alterado.</li>
 * </ol>
 *
 * <p><b>RESTRIÇÃO:</b> Não implementar lógica ainda. Apenas estrutura.
 *
 * @see MigracaoGdoor5
 * @see FabricaMigracao
 */
public class MigracaoGdoor6 implements AdaptadorMigracao {

    private static final String NOME = "MigracaoGdoor6";

    @Override
    public String getNome() {
        return NOME;
    }

    /**
     * Execução da migração Gdoor 6.x.
     *
     * <p><b>TODO (futuro):</b> Implementar quando o Gdoor 6.x for suportado.
     * Analisar:
     * <ul>
     *   <li>Novas tabelas ou campos do Gdoor 6.x no Firebird.</li>
     *   <li>Mudanças no esquema de tributação.</li>
     *   <li>Novos steps necessários além dos existentes no Gdoor 5.x.</li>
     * </ul>
     *
     * @param conn Conexão JDBC de origem (Firebird).
     * @throws UnsupportedOperationException sempre — estratégia não implementada.
     */
    @Override
    public void executar(Connection conn) throws Exception {
        throw new UnsupportedOperationException(
            "[" + NOME + "] Migração Gdoor 6.x ainda não implementada. "
            + "Estrutura criada para evolução futura."
        );
    }
}
