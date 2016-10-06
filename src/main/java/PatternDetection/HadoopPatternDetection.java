package PatternDetection;

public class HadoopPatternDetection {

    public static void main(String[] args) throws Exception {

        //Job One
        HadoopFindEntitiesWithBigrams findEntities = new HadoopFindEntitiesWithBigrams(args[0], "output/1/" + args[1]);

        HadoopEntityWordCount entityWordCount = new HadoopEntityWordCount("output/1/" + args[1], "output/2/" + args[1]);

    }

}