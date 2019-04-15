package com.powerutil.si;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Component("line_generator_bean")
public class LineGenerator implements Iterator<String> {

    @Autowired
    private ResourceLoader loader;

    @Value("${line.feed.source}")
    private String lineFeedSource;


    private List<String> myLines ;
    private int index = 0;
    @PostConstruct
    private  void init(){

        try{
            myLines = Files.lines(loader.getResource("classpath:"+lineFeedSource).getFile().toPath()).map(s -> s.trim())
                    .filter(s -> s.length() >0).collect(toList());

        }catch (Exception ex){
            System.out.println("Unable to read lines "+ ex.getLocalizedMessage());
        }
    }
    @Override
    public boolean hasNext() {
        return myLines != null;
    }

    @Override
    public String next() {
        index++;
        index = index % myLines.size();
        return myLines.get(index);
    }
}
