/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.virginia.biocomplexity.pansim_behavior;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.memory.RootAllocator;
import py4j.GatewayServer;

/**
 *
 * @author parantapa
 */
public class PansimBehaviorGateway {
    public static GatewayServer gatewayServer;
    
    public void testStateDataFrame() {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        StateDataFrameBuilder builder = new StateDataFrameBuilder(100000, allocator);
        builder.reset();
        for (int i = 0; i < 100 ; i++) {
            builder.pid.set(i, i);
            builder.group.set(i, 0);
            builder.current_state.set(i, 0);
            builder.next_state.set(i, -1);
            builder.dwell_time.set(i, -1);
            builder.seed.set(i, i);
        }
        builder.setValueCount(100);
        
        byte[] raw;
        try {
            raw = builder.toBytes();
        } catch (IOException ex) {
            Logger.getLogger(PansimBehaviorGateway.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        System.out.println(raw.length);
        
        StateDataFrameReader reader;
        try {
            reader = new StateDataFrameReader(raw, allocator);
        } catch (IOException ex) {
            Logger.getLogger(PansimBehaviorGateway.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        System.out.println(reader.schemaRoot.getRowCount());
        System.out.println(reader.schemaRoot.contentToTSVString());
    }
    
    public void testVisitDataFrame() {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        
        ArrayList<String> attr_names = new ArrayList<>();
        attr_names.add("attr_1");
        attr_names.add("attr_2");
        attr_names.add("attr_3");

        VisitDataFrameBuilder builder = new VisitDataFrameBuilder(attr_names, 1000000, allocator);
        builder.reset();
        for (int i = 0; i < 100 ; i++) {
            builder.lid.set(i, i);
            builder.pid.set(i, i);
            builder.group.set(i, 0);
            builder.state.set(i, 0);
            builder.behavior.set(i, i % 2);
            builder.start_time.set(i, i);
            builder.end_time.set(i, i + 100);
            for (String attr: attr_names) {
                builder.attrs.get(attr).set(i, 0);
            }
        }
        builder.setValueCount(100);
        
        byte[] raw;
        try {
            raw = builder.toBytes();
        } catch (IOException ex) {
            Logger.getLogger(PansimBehaviorGateway.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        System.out.println(raw.length);
        
        VisitDataFrameReader reader;
        try {
            reader = new VisitDataFrameReader(attr_names, raw, allocator);
        } catch (IOException ex) {
            Logger.getLogger(PansimBehaviorGateway.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        System.out.println(reader.schemaRoot.getRowCount());
        System.out.println(reader.schemaRoot.contentToTSVString());
    }
    
    public void testVisitOutputDataFrame() {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        
        ArrayList<String> attr_names = new ArrayList<>();
        attr_names.add("attr_1");
        attr_names.add("attr_2");
        attr_names.add("attr_3");

        VisitOutputDataFrameBuilder builder = new VisitOutputDataFrameBuilder(attr_names, 1000000, allocator);
        for (int i = 0; i < 100 ; i++) {
            builder.lid.set(i, i);
            builder.pid.set(i, i);
            builder.inf_prob.set(i, 1.0 / i);
            builder.n_contacts.set(i, i);
            for (String attr: attr_names) {
                builder.attrs.get(attr).set(i, 42);
            }
        }
        builder.setValueCount(100);
        
        byte[] raw;
        try {
            raw = builder.toBytes();
        } catch (IOException ex) {
            Logger.getLogger(PansimBehaviorGateway.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        System.out.println(raw.length);
        
        VisitOutputDataFrameReader reader;
        try {
            reader = new VisitOutputDataFrameReader(attr_names, raw, allocator);
        } catch (IOException ex) {
            Logger.getLogger(PansimBehaviorGateway.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        System.out.println(reader.schemaRoot.getRowCount());
        System.out.println(reader.schemaRoot.contentToTSVString());
    }

    public String processVisitOutputs(String state_ref, String visit_output_ref) {
        System.out.println(state_ref);
        System.out.println(visit_output_ref);
        
        return "Hello";
    }
    
    public void shutdown() {
        if (gatewayServer != null) {
            gatewayServer.shutdown();
            System.out.println("Gateway Server Shutdown");
        }
    }
    
    public static void main(String[] args) {
        // gatewayServer = new GatewayServer(new PansimBehaviorGateway());
        // gatewayServer.start();
        // System.out.println("Gateway Server Started");
        
        var g = new PansimBehaviorGateway();
        //g.testStateDataFrame();
        //g.testVisitDataFrame();
        g.testVisitOutputDataFrame();
    }
}
