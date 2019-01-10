package zhaoyi;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.List;

public class MyRockMapping implements DNSToSwitchMapping {

    public List<String> resolve(List<String> names) {
        // rack list
        List<String> racks = new ArrayList<String>();
        int ipNum = 0;// will be 132,133,134,135
        // 获取机架IP
        if(names != null && names.size() > 0){

            for (String name: names) {
                if(name.startsWith("h")){//host name
                    ipNum = Integer.parseInt(name.substring(1));
                }else{// ipv4
                    ipNum = Integer.parseInt(name.substring(1 + name.lastIndexOf(".")));
                }
            }

            if(ipNum <= 133){//132,133
                racks.add("/dd/rack1" );
            }else{//134,135
                racks.add("/dd/rack2");
            }

        }
        return racks;
    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> names) {

    }
}
