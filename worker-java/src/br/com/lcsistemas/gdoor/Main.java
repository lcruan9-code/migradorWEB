package br.com.lcsistemas.gdoor;

import br.com.lcsistemas.gdoor.ui.MainFrame;
import com.formdev.flatlaf.FlatLightLaf; // IMPORTANTE: Não esqueça esse import
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

/**
 * Ponto de entrada do GDOOR Migration Engine.
 * Inicia no Event Dispatch Thread do Swing.
 */
public class Main {

    public static void main(String[] args) {
        
        // 1. Aplica o look-and-feel do FlatLaf ANTES de abrir qualquer tela
        try {
            UIManager.setLookAndFeel(new FlatLightLaf());
            
            // Suas configurações de bordas arredondadas globais
            UIManager.put("Component.arc", 10);
            UIManager.put("ComboBox.arc", 10);
            UIManager.put("ToggleButton.arc", 10);
            UIManager.put("Button.arc", 10);
            UIManager.put("Dialog.arc", 10);
            UIManager.put("ScrollPane.arc", 10);
            
        } catch (Exception e) {
            System.err.println("Erro ao inicializar o tema FlatLaf: " + e.getMessage());
        }

        // 2. Abre a tela principal
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                new MainFrame().setVisible(true);
            }
        });
    }
}