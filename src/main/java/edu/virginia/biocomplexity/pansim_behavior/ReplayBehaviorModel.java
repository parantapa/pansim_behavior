/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.virginia.biocomplexity.pansim_behavior;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.memory.RootAllocator;

/**
 *
 * @author parantapa
 */
public class ReplayBehaviorModel {
    public ArrayList<String> visit_files;
    public ArrayList<String> attr_names;
    public int sim_ticks;
    public int max_visits;
    
    ReplayBehaviorModel(ArrayList<String> visit_files, ArrayList<String> attr_names, int sim_ticks, int max_visits) {
        this.visit_files = visit_files;
        this.attr_names = attr_names;
        this.sim_ticks = sim_ticks;
        this.max_visits = max_visits;
    }
    
    public byte[] getVisits(int tick, HashMap<Integer,Integer> state_map) {
        FileReader filereader;
        try {
            filereader = new FileReader(visit_files.get(tick));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ReplayBehaviorModel.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
        CSVReader csvReader = new CSVReader(filereader);
        List<String[]> lines;
        try {
            lines = csvReader.readAll();
        } catch (IOException ex) {
            Logger.getLogger(ReplayBehaviorModel.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        } catch (CsvException ex) {
            Logger.getLogger(ReplayBehaviorModel.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
        
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VisitDataFrameBuilder builder = new VisitDataFrameBuilder(attr_names, max_visits, allocator);
        
        for (int i=1; i < lines.size(); i++) {
            String[] line = lines.get(i);
            int pid = Integer.parseInt(line[0]);
            int lid = Integer.parseInt(line[1]);
            int start_time = Integer.parseInt(line[2]);
            int end_time = Integer.parseInt(line[3]);
            int group = 0;
            int state = state_map.get(pid);
            int behavior = 0;
            
            builder.lid.set(i-1, lid);
            builder.pid.set(i-1, pid);
            builder.group.set(i-1, group);
            builder.state.set(i-1, state);
            builder.behavior.set(i-1, behavior);
            builder.start_time.set(i-1, start_time);
            builder.end_time.set(i-1, end_time);
            for (String name: attr_names) {
                builder.attrs.get(name).set(i-1, 0);
            }
        }
        
        builder.setValueCount(lines.size() -1);
        byte[] raw = null;
        try {
            raw = builder.toBytes();
        } catch (IOException ex) {
            Logger.getLogger(ReplayBehaviorModel.class.getName()).log(Level.SEVERE, null, ex);
        }
        builder.close();
        
        return raw;
    }
}
