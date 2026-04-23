package br.com.lcsistemas.gdoor.versao;

/**
 * Enumeração das versões suportadas do Firebird.
 *
 * Regra de negócio:
 * - Detectada pelo {@link DetectorFirebird} em tempo de execução.
 * - DESCONHECIDA é o valor seguro retornado em caso de falha de detecção.
 * - Novas versões do Firebird podem ser adicionadas sem alterar código existente.
 */
public enum VersaoFirebird {

    FB25("Firebird 2.5"),
    FB30("Firebird 3.0"),
    FB40("Firebird 4.0"),
    FB50("Firebird 5.0"),
    DESCONHECIDA("Versão Desconhecida");

    private final String descricao;

    VersaoFirebird(String descricao) {
        this.descricao = descricao;
    }

    public String getDescricao() {
        return descricao;
    }

    @Override
    public String toString() {
        return descricao;
    }
}
