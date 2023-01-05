package framework.Anonymization;

import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.ARXAnonymizer;
import org.deidentifier.arx.ARXConfiguration;
import org.deidentifier.arx.AttributeType;
import org.deidentifier.arx.Data;
import org.deidentifier.arx.criteria.KAnonymity;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class AnonymizationConfiguration {
    public static final Charset charset = StandardCharsets.UTF_8;
    public static final char hierarchyValueDelimiter = ';';
    private String anonymizationTechnique ;
    private ArrayList<String> insensitiveArgs;
    private int k;
    private int l;
    private int t;
    private ARXConfiguration config;

    public AnonymizationConfiguration(XMLConfiguration config){
        anonymizationTechnique = config.getString("technique");
        if (config.containsKey("k")){
            k = config.getInt("k");
        }
        if (config.containsKey("l")){
            k = config.getInt("l");
        }
        if (config.containsKey("t")){
            k = config.getInt("t");
        }
        insensitiveArgs = new ArrayList<>(Arrays.asList(config.getStringArray("InsensitiveArgs")));
    }

    public int getK() {
        return k;
    }

    public String getAnonymizationTechnique() {
        return anonymizationTechnique;
    }

    public ARXConfiguration getARXConfig() {
        return config;
    }

    public void create(){
        this.config = ARXConfiguration.create();
        switch (anonymizationTechnique){
            case "k":
                config.addPrivacyModel(new KAnonymity(2));
        }
    }

    public void applyToData(Data data){
        for (String s: insensitiveArgs){
            data.getDefinition().setAttributeType(s, AttributeType.INSENSITIVE_ATTRIBUTE);
        }
    }
}
