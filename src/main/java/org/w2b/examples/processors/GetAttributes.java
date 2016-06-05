/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.w2b.examples.processors;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * GetAttributes: To read attributes from local file
 */
@SideEffectFree @Tags({"Read Attributes"}) @CapabilityDescription("Read attributes from local file")
public class GetAttributes extends AbstractProcessor {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final PropertyDescriptor LOCAL_FILE_PATH =
        new PropertyDescriptor.Builder().name("Local Path").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final Relationship REL_SUCCESS =
        new Relationship.Builder().name("SUCCESS").description("Succes relationship").build();

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(LOCAL_FILE_PATH);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Builds the Map of attributes that should be included in the attributes that is emitted from
     * this process.
     *
     * @return
     *  Map of key,values that are added to attributes of flowfile.
     */
    public Map<String, String> buildAttributesMapForFlowFile(String path) throws Exception {
        try {
            Map<String, String> map = new HashMap<>();
            BufferedReader in = new BufferedReader(new FileReader(path));
            String line = "";
            while ((line = in.readLine()) != null) {
                map.put(line.split("=")[0], line.split("=")[1]);
            }
            return map;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    @Override public void onTrigger(final ProcessContext context, final ProcessSession session)
        throws ProcessException {
        final ProcessorLog log = this.getLogger();
        final AtomicReference<Map<String, String>> value = new AtomicReference<>();

        FlowFile flowfile = session.get();

        if (flowfile == null) {
            return;
        }

        session.read(flowfile, new InputStreamCallback() {
            @Override public void process(InputStream in) throws IOException {
                try {
                    Map<String, String> result =
                        buildAttributesMapForFlowFile(context.getProperty(LOCAL_FILE_PATH).toString());
                    value.set(result);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    log.error("Failed to read local file.");
                }
            }
        });

        // Write the results to an attribute 
        Map<String, String> results = value.get();
        if (results != null && !results.isEmpty()) {
            flowfile = session.putAllAttributes(flowfile, results);
        }


        session.transfer(flowfile, REL_SUCCESS);
    }

}
