package com.quark.datastream.runtime.task.ml_svm;

import com.google.gson.Gson;
import com.quark.datastream.runtime.task.DataSet;
import com.quark.datastream.runtime.task.TaskNodeParam;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;


public class MultiClassificationNodeTest {

    String sv = "0.5138998581245972 0.7177010062266509 0.0 1:0.708333 2:1.0 3:1.0 4:-0.320755 5:-0.105023 6:-1.0 7:1.0 8:-0.419847 9:-1.0 10:-0.225806 12:1.0 13:-1.0 \n" +
            "0.0 0.0572316760550494 0.0 2:1.0 3:1.0 4:-0.132075 5:-0.648402 6:1.0 7:1.0 8:0.282443 9:1.0 11:1.0 12:-1.0 13:1.0 \n" +
            "0.0 0.2210572206311982 0.0 1:0.583333 2:1.0 3:1.0 4:-0.509434 5:-0.52968 6:-1.0 7:1.0 8:-0.114504 9:1.0 10:-0.16129 12:0.333333 13:1.0 \n" +
            "1.0 0.0 0.0 1:0.208333 2:1.0 3:0.333333 4:-0.660377 5:-0.525114 6:-1.0 7:1.0 8:0.435115 9:-1.0 10:-0.193548 12:-0.333333 13:1.0 \n" +
            "1.0 0.0 0.0 1:0.166667 2:1.0 3:0.333333 4:-0.358491 5:-0.52968 6:-1.0 7:1.0 8:0.206107 9:-1.0 10:-0.870968 12:-0.333333 13:1.0 \n" +
            "0.0 0.30083897442852736 0.0 1:0.25 2:1.0 3:1.0 4:0.433962 5:-0.086758 6:-1.0 7:1.0 8:0.0534351 9:1.0 10:0.0967742 11:1.0 12:-1.0 13:1.0 \n" +
            "0.0 0.19001802183028937 0.0 1:0.333333 2:1.0 3:1.0 4:-0.132075 5:-0.630137 6:-1.0 7:1.0 8:0.0229008 9:1.0 10:-0.387097 11:-1.0 12:-0.333333 13:1.0 \n" +
            "1.0 1.0 0.9626193911836644 1:0.25 2:1.0 3:-1.0 4:0.245283 5:-0.328767 6:-1.0 7:1.0 8:-0.175573 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "1.0 0.0 0.6771482547301124 1:-0.541667 2:1.0 3:1.0 4:0.0943396 5:-0.557078 6:-1.0 7:-1.0 8:0.679389 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:1.0 \n" +
            "1.0 0.29067827203956764 0.404618549852803 1:0.25 2:1.0 3:0.333333 4:-0.396226 5:-0.579909 6:1.0 7:-1.0 8:-0.0381679 9:-1.0 10:-0.290323 12:-0.333333 13:0.5 \n" +
            "1.0 0.0 0.053747492842501916 1:-0.375 2:1.0 3:1.0 4:-0.698113 5:-0.675799 6:-1.0 7:1.0 8:0.618321 9:-1.0 10:-1.0 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "0.45354553677931075 0.677839431854524 0.0 1:0.541667 2:1.0 3:-0.333333 4:0.245283 5:-0.452055 6:-1.0 7:-1.0 8:-0.251908 9:1.0 10:-1.0 12:1.0 13:0.5 \n" +
            "1.0 0.0 0.0 1:0.333333 2:1.0 3:1.0 4:-0.169811 5:-0.817352 6:-1.0 7:1.0 8:-0.175573 9:1.0 10:0.16129 12:-0.333333 13:-1.0 \n" +
            "1.0 0.41886725611469683 0.0 1:0.375 2:1.0 3:-0.333333 4:-0.509434 5:-0.292237 6:-1.0 7:1.0 8:-0.51145 9:-1.0 10:-0.548387 12:-0.333333 13:1.0 \n" +
            "1.0 0.34778929681676957 0.0 1:-0.0833333 2:-1.0 3:1.0 4:-0.320755 5:-0.182648 6:-1.0 7:-1.0 8:0.0839695 9:1.0 10:-0.612903 12:-1.0 13:1.0 \n" +
            "1.0 0.4397972060688852 0.6835919222130267 1:0.208333 2:-1.0 3:-0.333333 4:-0.207547 5:-0.118721 6:1.0 7:1.0 8:0.236641 9:-1.0 10:-1.0 11:-1.0 12:0.333333 13:-1.0 \n" +
            "1.0 0.6673092488355096 1.0 1:-0.25 2:1.0 3:0.333333 4:-0.735849 5:-0.465753 6:-1.0 7:-1.0 8:0.236641 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "0.0 0.0 0.36915379545220756 1:-0.75 2:1.0 3:1.0 4:-0.509434 5:-0.671233 6:-1.0 7:-1.0 8:-0.0992366 9:1.0 10:-0.483871 12:-1.0 13:1.0 \n" +
            "0.0 0.17427643352691757 0.0 1:0.208333 2:1.0 3:1.0 4:0.0566038 5:-0.342466 6:-1.0 7:1.0 8:-0.389313 9:1.0 10:-0.741935 11:-1.0 12:-1.0 13:1.0 \n" +
            "1.0 0.0 1.0 1:0.333333 2:-1.0 3:1.0 4:-0.320755 5:-0.0684932 6:-1.0 7:1.0 8:0.496183 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "1.0 0.42729319535705296 0.9025514526743352 1:0.0416667 2:1.0 3:1.0 4:-0.698113 5:-0.634703 6:-1.0 7:1.0 8:-0.435115 9:1.0 10:-1.0 12:-0.333333 13:-1.0 \n" +
            "1.0 0.5201104800146787 0.0 1:-0.0416667 2:1.0 3:1.0 4:-0.415094 5:-0.607306 6:-1.0 7:-1.0 8:0.480916 9:-1.0 10:-0.677419 11:-1.0 12:0.333333 13:1.0 \n" +
            "1.0 0.0 0.0 1:-0.25 2:1.0 3:1.0 4:-0.698113 5:-0.319635 6:-1.0 7:1.0 8:-0.282443 9:1.0 10:-0.677419 12:-0.333333 13:-1.0 \n" +
            "1.0 0.07909901883464553 1.0 1:0.208333 2:1.0 3:1.0 4:-0.886792 5:-0.506849 6:-1.0 7:-1.0 8:0.29771 9:-1.0 10:-0.967742 11:-1.0 12:-0.333333 13:1.0 \n" +
            "1.0 0.0 0.0 1:-0.208333 2:1.0 3:1.0 4:-0.433962 5:-0.324201 6:-1.0 7:1.0 8:0.450382 9:-1.0 10:-0.83871 12:-1.0 13:1.0 \n" +
            "0.0 0.4690556937252496 1.0 1:0.25 2:1.0 3:1.0 4:-0.132075 5:-0.767123 6:-1.0 7:-1.0 8:0.389313 9:1.0 10:-1.0 11:-1.0 12:-0.333333 13:1.0 \n" +
            "1.0 0.21318992414912066 0.39028853131643526 1:0.0833333 2:-1.0 3:1.0 4:0.622642 5:-0.0821918 6:-1.0 8:-0.29771 9:1.0 10:0.0967742 12:-1.0 13:-1.0 \n" +
            "1.0 0.0 1.0 1:0.291667 2:-1.0 3:1.0 4:0.207547 5:-0.182648 6:-1.0 7:1.0 8:0.374046 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "1.0 0.0 0.0 1:-0.291667 2:1.0 3:1.0 4:-0.509434 5:-0.438356 6:-1.0 7:1.0 8:0.114504 9:-1.0 10:-0.741935 11:-1.0 12:-1.0 13:1.0 \n" +
            "0.0 0.3839438130787075 0.0 1:0.375 2:1.0 3:1.0 4:-0.509434 5:-0.356164 6:-1.0 7:-1.0 8:-0.572519 9:1.0 10:-0.419355 12:0.333333 13:1.0 \n" +
            "0.8199092295583822 0.0 0.0 1:0.291667 2:1.0 3:1.0 4:-0.566038 5:-0.525114 6:1.0 7:-1.0 8:0.358779 9:1.0 10:-0.548387 11:-1.0 12:0.333333 13:1.0 \n" +
            "1.0 0.0 0.8429586382987245 1:0.541667 2:1.0 3:1.0 4:-0.660377 5:-0.607306 6:-1.0 7:1.0 8:-0.0687023 9:1.0 10:-0.967742 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "0.06685433076193481 0.1027886461103987 0.0 1:0.458333 2:1.0 3:1.0 4:-0.509434 5:-0.452055 6:-1.0 7:1.0 8:-0.618321 9:1.0 10:-0.290323 11:1.0 12:-0.333333 13:-1.0 \n" +
            "1.0 0.07144733365356029 0.0 1:0.125 2:1.0 3:1.0 4:-0.415094 5:-0.438356 6:1.0 7:1.0 8:0.114504 9:1.0 10:-0.612903 12:-0.333333 13:-1.0 \n" +
            "1.0 0.0 0.0 1:-0.125 2:1.0 3:0.333333 4:-0.132075 5:-0.511416 6:-1.0 7:-1.0 8:0.40458 9:-1.0 10:-0.806452 12:-0.333333 13:1.0 \n" +
            "1.0 0.0 0.0 1:0.0416667 2:1.0 3:-0.333333 4:0.849057 5:-0.283105 6:-1.0 7:1.0 8:0.89313 9:-1.0 10:-1.0 11:-1.0 12:-0.333333 13:1.0 \n" +
            "1.0 0.320067310376608 1.0 1:-0.0416667 2:1.0 3:1.0 4:-0.660377 5:-0.525114 6:-1.0 7:-1.0 8:0.358779 9:-1.0 10:-1.0 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "0.0 1.0 0.0 1:-0.541667 2:1.0 3:1.0 4:-0.698113 5:-0.812785 6:-1.0 7:1.0 8:-0.343511 9:1.0 10:-0.354839 12:-1.0 13:1.0 \n" +
            "0.0 0.19022559678598455 0.0 1:0.208333 2:1.0 3:0.333333 4:-0.283019 5:-0.552511 6:-1.0 7:1.0 8:0.557252 9:-1.0 10:0.0322581 11:-1.0 12:0.333333 13:1.0 \n" +
            "0.036850512194288636 0.0 0.0 1:0.541667 2:-1.0 3:1.0 4:0.584906 5:-0.534247 6:1.0 7:-1.0 8:0.435115 9:1.0 10:-0.677419 12:0.333333 13:1.0 \n" +
            "1.0 0.16215702077572383 0.4504548008156315 1:-0.625 2:1.0 3:-1.0 4:-0.509434 5:-0.520548 6:-1.0 7:-1.0 8:0.694656 9:1.0 10:0.225806 12:-1.0 13:1.0 \n" +
            "1.0 1.0 1.0 1:0.375 2:-1.0 3:1.0 4:0.0566038 5:-0.461187 6:-1.0 7:-1.0 8:0.267176 9:1.0 10:-0.548387 12:-1.0 13:-1.0 \n" +
            "0.0 0.2190144547422159 0.0 1:0.666667 2:1.0 3:0.333333 4:-0.132075 5:-0.415525 6:-1.0 7:1.0 8:0.145038 9:-1.0 10:-0.354839 12:1.0 13:1.0 \n" +
            "0.0 0.4008420477706439 0.0 1:0.583333 2:1.0 3:1.0 4:-0.886792 5:-0.210046 6:-1.0 7:1.0 8:-0.175573 9:1.0 10:-0.709677 12:0.333333 13:-1.0 \n" +
            "1.0 0.27379866487583904 0.0 1:0.375 2:-1.0 3:1.0 4:-0.169811 5:-0.232877 6:1.0 7:-1.0 8:-0.465649 9:-1.0 10:-0.387097 12:1.0 13:-1.0 \n" +
            "0.0 0.2989252520477516 0.0 1:-0.0833333 2:1.0 3:1.0 4:-0.132075 5:-0.214612 6:-1.0 7:-1.0 8:-0.221374 9:1.0 10:0.354839 12:1.0 13:1.0 \n" +
            "1.0 0.8577367480205434 0.8894714217526546 1:-0.291667 2:1.0 3:0.333333 4:0.0566038 5:-0.520548 6:-1.0 7:-1.0 8:0.160305 9:-1.0 10:0.16129 12:-1.0 13:-1.0 \n" +
            "1.0 0.0 0.0 1:0.583333 2:1.0 3:1.0 4:-0.415094 5:-0.415525 6:1.0 7:-1.0 8:0.40458 9:-1.0 10:-0.935484 12:0.333333 13:1.0 \n" +
            "1.0 0.0 0.05306650514808217 1:-0.5 2:1.0 3:1.0 4:-0.698113 5:-0.789954 6:-1.0 7:1.0 8:0.328244 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:1.0 \n" +
            "0.0 0.10510586828335308 0.0 1:0.708333 2:1.0 3:1.0 4:-0.0377358 5:-0.780822 6:-1.0 7:-1.0 8:-0.175573 9:1.0 10:-0.16129 11:1.0 12:-1.0 13:1.0 \n" +
            "0.8404113073558135 0.11875322607496783 0.0 1:-0.75 2:1.0 3:1.0 4:-0.396226 5:-0.287671 6:-1.0 7:1.0 8:0.29771 9:1.0 10:-1.0 11:-1.0 12:-1.0 13:1.0 \n" +
            "0.3816948018015267 0.06040376124602548 0.0 1:1.0 2:1.0 3:1.0 4:-0.415094 5:-0.187215 6:-1.0 7:1.0 8:0.389313 9:1.0 10:-1.0 11:-1.0 12:1.0 13:-1.0 \n" +
            "1.0 0.2220880237435645 0.0 1:-0.0833333 2:1.0 3:1.0 4:-0.132075 5:-0.210046 6:-1.0 7:-1.0 8:0.557252 9:1.0 10:-0.483871 11:-1.0 12:-1.0 13:1.0 \n" +
            "0.811199007006819 0.28285329469327947 1.0 1:0.458333 2:1.0 3:0.333333 4:-0.415094 5:-0.164384 6:-1.0 7:-1.0 8:-0.0839695 9:1.0 10:-0.419355 12:-1.0 13:1.0 \n" +
            "1.0 0.0 0.0 1:0.25 2:1.0 3:-1.0 4:0.433962 5:-0.260274 6:-1.0 7:1.0 8:0.343511 9:-1.0 10:-0.935484 12:-1.0 13:1.0 \n" +
            "0.0 1.0 0.0 1:0.0416667 2:1.0 3:1.0 4:-0.698113 5:-0.484018 6:-1.0 7:-1.0 8:-0.160305 9:1.0 10:-0.0967742 12:-0.333333 13:1.0 \n" +
            "1.0 0.06328417293041069 0.8251850118811842 1:0.375 2:-1.0 3:0.333333 4:-0.320755 5:-0.374429 6:-1.0 7:-1.0 8:-0.603053 9:-1.0 10:-0.612903 12:-0.333333 13:1.0 \n" +
            "0.7198143799470645 0.0 0.0 1:-0.416667 2:-1.0 3:1.0 4:-0.283019 5:-0.0182648 6:1.0 7:1.0 8:-0.00763359 9:1.0 10:-0.0322581 12:-1.0 13:1.0 \n" +
            "0.8487918629057276 0.0 0.2512119406593686 1:0.333333 2:-1.0 3:1.0 4:-0.0377358 5:-0.173516 6:-1.0 7:1.0 8:0.145038 9:1.0 10:-0.677419 12:-1.0 13:1.0 \n" +
            "1.0 0.0 0.38362316665530233 1:-0.583333 2:1.0 3:1.0 4:-0.54717 5:-0.575342 6:-1.0 7:-1.0 8:0.0534351 9:-1.0 10:-0.612903 12:-1.0 13:1.0 \n" +
            "0.0 0.09088983458543845 0.34619855131715693 1:-0.0416667 2:1.0 3:1.0 4:-0.358491 5:-0.410959 6:-1.0 7:-1.0 8:0.374046 9:1.0 10:-1.0 11:-1.0 12:-0.333333 13:1.0 \n" +
            "1.0 0.34299647430871677 0.0 1:0.625 2:1.0 3:0.333333 4:0.622642 5:-0.324201 6:1.0 7:1.0 8:0.206107 9:1.0 10:-0.483871 12:-1.0 13:1.0 \n" +
            "1.0 0.0629845539689178 0.007347115769412795 1:0.375 2:-1.0 3:1.0 4:-0.132075 5:-0.351598 6:-1.0 7:1.0 8:0.358779 9:-1.0 10:0.16129 11:1.0 12:0.333333 13:-1.0 \n" +
            "1.0 0.3564762650361958 0.39609389114435567 1:0.291667 2:1.0 3:0.333333 4:-0.132075 5:-0.730594 6:-1.0 7:1.0 8:0.282443 9:-1.0 10:-0.0322581 12:-1.0 13:-1.0 \n" +
            "0.0 0.13431869679095318 0.0 1:0.0416667 2:1.0 3:1.0 4:-0.509434 5:-0.716895 6:-1.0 7:-1.0 8:-0.358779 9:-1.0 10:-0.548387 12:-0.333333 13:1.0 \n" +
            "1.0 0.0 0.0 1:-0.375 2:1.0 3:1.0 4:-0.660377 5:-0.251142 6:-1.0 7:1.0 8:0.251908 9:-1.0 10:-1.0 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "1.0 0.10103397222964726 1.0 1:0.458333 2:1.0 3:0.333333 4:-0.132075 5:-0.0456621 6:-1.0 7:-1.0 8:0.328244 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "1.0 0.5657126113612196 0.11066956629304046 1:-0.208333 2:1.0 3:-0.333333 4:-0.698113 5:-0.52968 6:-1.0 7:-1.0 8:0.480916 9:-1.0 10:-0.677419 11:1.0 12:-1.0 13:1.0 \n" +
            "-1.0 0.0032371334489660598 0.0 1:0.583333 2:-1.0 3:0.333333 4:-0.603774 5:1.0 6:-1.0 7:1.0 8:0.358779 9:-1.0 10:-0.483871 12:-1.0 13:1.0 \n" +
            "-1.0 1.0 0.2731552963697384 2:1.0 3:1.0 4:-0.0943396 5:-0.543379 6:-1.0 7:1.0 8:-0.389313 9:1.0 10:-1.0 11:-1.0 12:-1.0 13:1.0 \n" +
            "-1.0 0.0 0.008411363940745176 1:0.166667 2:-1.0 3:1.0 4:-0.358491 5:-0.191781 6:-1.0 7:1.0 8:0.343511 9:-1.0 10:-1.0 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "-1.0 0.5894429775628125 0.05605871055948846 1:-0.541667 2:1.0 3:-1.0 4:-0.132075 5:-0.666667 6:-1.0 7:-1.0 8:0.633588 9:1.0 10:-0.548387 11:-1.0 12:-1.0 13:1.0 \n" +
            "-0.7368866951745194 0.0 0.0 1:-0.416667 2:1.0 3:1.0 4:-0.603774 5:-0.191781 6:-1.0 7:-1.0 8:0.679389 9:-1.0 10:-0.612903 12:-1.0 13:-1.0 \n" +
            "-0.7743182397865477 0.0 0.040773014203688916 1:0.0416667 2:-1.0 3:-0.333333 4:-0.283019 5:-0.260274 6:1.0 7:1.0 8:0.343511 9:1.0 10:-1.0 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "-0.43001926401217283 0.0 0.0 1:0.75 2:-1.0 3:0.333333 4:-0.698113 5:-0.365297 6:1.0 7:1.0 8:-0.0992366 9:-1.0 10:-1.0 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "-1.0 0.6068554956448733 0.0 1:0.541667 2:1.0 3:1.0 4:0.245283 5:-0.534247 6:-1.0 7:1.0 8:0.0229008 9:-1.0 10:-0.258065 11:-1.0 12:-1.0 13:0.5 \n" +
            "-1.0 0.0 0.19407417400533833 1:-0.208333 2:1.0 3:1.0 4:-0.471698 5:-0.561644 6:-1.0 7:1.0 8:0.755725 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.0 0.0 0.405765933668345 1:0.375 2:-1.0 3:1.0 4:-0.433962 5:-0.621005 6:-1.0 7:-1.0 8:0.40458 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.0 0.0 0.14863599946746986 1:0.208333 2:1.0 3:0.333333 4:-0.132075 5:-0.611872 6:1.0 7:1.0 8:0.435115 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.0 0.0 0.2214830772662334 1:-0.0416667 2:1.0 3:-0.333333 4:-0.358491 5:-0.639269 6:1.0 7:-1.0 8:0.725191 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.32030071746721 0.0 0.0 1:0.5 2:-1.0 3:0.333333 4:-0.132075 5:0.328767 6:1.0 7:1.0 8:0.312977 9:-1.0 10:-0.741935 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "-0.0 0.0 0.19752448440415443 1:0.291667 2:-1.0 3:0.333333 4:-0.509434 5:-0.762557 6:1.0 7:-1.0 8:-0.618321 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.17093772715044897 1:0.166667 2:1.0 3:0.333333 4:0.0566038 5:-1.0 6:1.0 7:-1.0 8:0.557252 9:-1.0 10:-0.935484 11:-1.0 12:-0.333333 13:1.0 \n" +
            "-0.9928791680103163 0.04705693883363037 0.0 1:0.416667 2:1.0 3:-1.0 4:-0.0377358 5:-0.511416 6:1.0 7:1.0 8:0.206107 9:-1.0 10:-0.258065 11:1.0 12:-1.0 13:0.5 \n" +
            "-1.0 0.0 0.0 1:-0.0833333 2:1.0 3:1.0 4:-0.132075 5:-0.383562 6:-1.0 7:1.0 8:0.755725 9:1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.5617909582571726 0.38074450583341646 0.43885093603213077 1:0.166667 2:-1.0 3:1.0 4:-0.509434 5:0.0410959 6:-1.0 7:-1.0 8:0.40458 9:1.0 10:-0.806452 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.0 0.0 0.06333009376975156 1:0.708333 2:1.0 3:-0.333333 4:0.169811 5:-0.456621 6:-1.0 7:1.0 8:0.0992366 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.0 0.0 0.5107349443542294 1:0.958333 2:-1.0 3:0.333333 4:-0.132075 5:-0.675799 6:-1.0 8:-0.312977 9:-1.0 10:-0.645161 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.26480964194183243 1:0.583333 2:-1.0 3:1.0 4:-0.773585 5:-0.557078 6:-1.0 7:-1.0 8:0.0839695 9:-1.0 10:-0.903226 11:-1.0 12:0.333333 13:-1.0 \n" +
            "-1.0 1.0 0.0 1:-0.333333 2:1.0 3:1.0 4:-0.811321 5:-0.625571 6:-1.0 7:1.0 8:0.175573 9:1.0 10:-0.0322581 12:-1.0 13:-1.0 \n" +
            "-0.0 0.0 0.0794059564279004 1:-0.583333 2:-1.0 3:0.333333 4:-1.0 5:-0.666667 6:-1.0 7:-1.0 8:0.648855 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.407946256926736 0.0 0.0 1:-0.5 2:1.0 3:0.333333 4:-0.320755 5:-0.598174 6:-1.0 7:1.0 8:0.480916 9:-1.0 10:-0.354839 12:-1.0 13:-1.0 \n" +
            "-0.6653016483290722 0.6514444959423461 0.0 1:-0.458333 2:1.0 3:-1.0 4:0.0188679 5:-0.461187 6:-1.0 7:1.0 8:0.633588 9:-1.0 10:-0.741935 11:-1.0 12:0.333333 13:-1.0 \n" +
            "-1.0 0.5309448892389429 0.0 1:0.25 2:1.0 3:-1.0 4:0.584906 5:-0.342466 6:-1.0 7:1.0 8:0.129771 9:-1.0 10:0.354839 11:1.0 12:-1.0 13:1.0 \n" +
            "-0.0 0.0 0.6348997835117746 1:0.25 2:1.0 3:-0.333333 4:-0.132075 5:-0.56621 6:-1.0 7:-1.0 8:0.419847 9:1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.33058752722370666 1:0.541667 2:1.0 3:1.0 4:-0.509434 5:-0.196347 6:-1.0 7:1.0 8:0.221374 9:-1.0 10:-0.870968 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.29546860611370457 1:0.458333 2:-1.0 3:0.333333 4:-0.132075 5:-0.146119 6:-1.0 7:-1.0 8:-0.0534351 9:-1.0 10:-0.935484 11:-1.0 12:-1.0 13:1.0 \n" +
            "-0.0 0.0 0.34513899403444315 1:-0.375 2:-1.0 3:0.333333 4:-0.735849 5:-0.931507 6:-1.0 7:-1.0 8:0.587786 9:-1.0 10:-0.806452 12:-1.0 13:-1.0 \n" +
            "-1.0 1.0 1.0 1:-0.0833333 2:1.0 3:0.333333 4:-0.886792 5:-0.561644 6:-1.0 7:-1.0 8:0.0992366 9:1.0 10:-0.612903 12:-1.0 13:-1.0 \n" +
            "-0.0 0.20208430766609634 0.0 1:0.541667 2:-1.0 3:-1.0 4:0.0566038 5:-0.543379 6:-1.0 7:-1.0 8:-0.343511 9:-1.0 10:-0.16129 11:1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 0.8020851812689248 0.0 1:0.0416667 2:1.0 3:0.333333 4:-0.415094 5:-0.328767 6:-1.0 7:1.0 8:0.236641 9:-1.0 10:-0.83871 11:1.0 12:-0.333333 13:-1.0 \n" +
            "-0.0 0.05853389432954736 0.0 1:-0.0416667 2:1.0 3:-0.333333 4:-0.245283 5:-0.657534 6:-1.0 7:-1.0 8:0.328244 9:-1.0 10:-0.741935 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "-0.0 0.0 0.5304443198611298 1:-0.458333 2:1.0 3:1.0 4:-0.132075 5:-0.543379 6:-1.0 7:-1.0 8:0.633588 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.0 1:0.0416667 2:1.0 3:0.333333 4:0.0566038 5:-0.515982 6:-1.0 7:1.0 8:0.435115 9:-1.0 10:-0.483871 11:-1.0 12:-1.0 13:1.0 \n" +
            "-1.0 0.2830164967933675 0.22322977183914136 1:-0.291667 2:-1.0 3:0.333333 4:-0.0943396 5:-0.767123 6:-1.0 7:1.0 8:0.358779 9:1.0 10:-0.548387 11:1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 1.0 1.0 1:0.166667 2:1.0 3:1.0 4:-0.283019 5:-0.630137 6:-1.0 7:-1.0 8:0.480916 9:1.0 10:-1.0 11:-1.0 12:-1.0 13:1.0 \n" +
            "-1.0 0.11356257753185349 0.7231950475379875 1:-0.0833333 2:1.0 3:-1.0 4:-0.415094 5:-0.60274 6:-1.0 7:1.0 8:-0.175573 9:1.0 10:-0.548387 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "-0.0 0.0 0.3916777611281937 1:-0.5 2:-1.0 3:0.333333 4:-0.660377 5:-0.351598 6:-1.0 7:1.0 8:0.541985 9:1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.0 0.0 0.46940219804846134 1:0.0416667 2:-1.0 3:0.333333 4:-0.735849 5:-0.356164 6:-1.0 7:1.0 8:0.465649 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 1.0 0.0 1:0.458333 2:-1.0 3:1.0 4:-0.320755 5:-0.191781 6:-1.0 7:-1.0 8:-0.221374 9:-1.0 10:-0.354839 12:0.333333 13:-1.0 \n" +
            "-1.0 0.0 0.0 1:0.125 2:1.0 3:-1.0 4:-0.509434 5:-0.694064 6:-1.0 7:1.0 8:0.389313 9:-1.0 10:-0.387097 12:-1.0 13:1.0 \n" +
            "-1.0 0.0 1.0 1:-0.416667 2:1.0 3:1.0 4:-0.698113 5:-0.611872 6:-1.0 7:-1.0 8:0.374046 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:1.0 \n" +
            "-1.0 0.6838365204481966 0.0 1:0.458333 2:-1.0 3:1.0 4:0.622642 5:-0.0913242 6:-1.0 7:-1.0 8:0.267176 9:1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.7698812540540922 1:-0.333333 2:-1.0 3:1.0 4:-0.169811 5:-0.497717 6:-1.0 7:1.0 8:0.236641 9:1.0 10:-0.935484 12:-1.0 13:-1.0 \n" +
            "-0.0 0.6332951345096421 0.0 1:0.666667 2:1.0 3:-1.0 4:0.245283 5:-0.506849 6:1.0 7:1.0 8:-0.0839695 9:-1.0 10:-0.967742 12:-0.333333 13:-1.0 \n" +
            "-0.0 0.0 0.3995797447476523 1:0.625 2:-1.0 3:0.333333 4:-0.509434 5:-0.611872 6:-1.0 7:1.0 8:-0.328244 9:-1.0 10:-0.516129 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.0 1:-0.458333 2:1.0 3:0.333333 4:-0.509434 5:-0.479452 6:1.0 7:-1.0 8:0.877863 9:-1.0 10:-0.741935 11:1.0 12:-1.0 13:1.0 \n" +
            "-1.0 0.3049028394098254 0.0 2:1.0 3:0.333333 4:-0.320755 5:-0.452055 6:1.0 7:1.0 8:0.557252 9:-1.0 10:-1.0 11:-1.0 12:1.0 13:-1.0 \n" +
            "-1.0 0.21944209997963263 0.0 1:-0.416667 2:1.0 3:0.333333 4:-0.320755 5:-0.136986 6:-1.0 7:-1.0 8:0.389313 9:-1.0 10:-0.387097 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "-1.0 0.6765342887031613 0.09746661710232457 1:-0.0416667 2:1.0 3:1.0 4:-0.735849 5:-0.511416 6:1.0 7:-1.0 8:0.160305 9:-1.0 10:-0.967742 11:-1.0 12:1.0 13:1.0 \n" +
            "-0.42545537110201104 0.0 0.0 1:0.375 2:-1.0 3:1.0 4:-0.132075 5:0.223744 6:-1.0 7:1.0 8:0.312977 9:-1.0 10:-0.612903 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 1.0 1:0.0416667 2:1.0 3:1.0 4:-0.132075 5:-0.484018 6:-1.0 7:-1.0 8:0.358779 9:-1.0 10:-0.612903 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 0.20995255070725966 0.6602646607588306 1:0.0416667 2:1.0 3:-0.333333 4:-0.735849 5:-0.164384 6:-1.0 7:-1.0 8:0.29771 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:1.0 \n" +
            "-0.0 0.0 0.11933950379215418 1:0.0833333 2:-1.0 3:-0.333333 4:-0.226415 5:-0.43379 6:-1.0 7:1.0 8:0.374046 9:-1.0 10:-0.548387 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.0 1:0.208333 2:-1.0 3:1.0 4:-0.886792 5:-0.442922 6:-1.0 7:1.0 8:-0.221374 9:-1.0 10:-0.677419 12:-1.0 13:-1.0 \n" +
            "-0.0 0.2232591142047341 0.0 1:0.666667 2:-1.0 3:-1.0 4:-0.132075 5:-0.484018 6:-1.0 7:-1.0 8:0.221374 9:-1.0 10:-0.419355 11:-1.0 12:0.333333 13:-1.0 \n" +
            "-1.0 1.0 0.764567436560278 1:0.208333 2:1.0 3:0.333333 4:-0.792453 5:-0.479452 6:-1.0 7:1.0 8:0.267176 9:1.0 10:-0.806452 12:-1.0 13:1.0 \n" +
            "-0.4020303910807952 0.10167943978247315 0.0 1:-0.666667 2:1.0 3:0.333333 4:-0.320755 5:-0.43379 6:-1.0 7:-1.0 8:0.770992 9:-1.0 10:0.129032 11:1.0 12:-1.0 13:-1.0 \n" +
            "-0.0 0.0 0.5431596794905865 1:-0.5 2:-1.0 3:-0.333333 4:-0.320755 5:-0.643836 6:-1.0 7:1.0 8:0.541985 9:-1.0 10:-0.548387 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.0 0.0 0.40644076012319724 1:0.416667 2:-1.0 3:0.333333 4:-0.226415 5:-0.424658 6:-1.0 7:1.0 8:0.541985 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.0 1:0.0416667 2:1.0 3:0.333333 4:-0.509434 5:-0.39726 6:-1.0 7:1.0 8:0.160305 9:-1.0 10:-0.870968 12:-1.0 13:1.0 \n" +
            "-1.0 0.2190168486218335 0.0 1:-0.5 2:1.0 3:-0.333333 4:-0.226415 5:-0.648402 6:-1.0 7:-1.0 8:-0.0687023 9:-1.0 10:-1.0 12:-1.0 13:0.5 \n" +
            "-0.19915123920215969 0.0 0.0 1:-0.0416667 2:1.0 3:-1.0 4:-0.54717 5:-0.726027 6:-1.0 7:1.0 8:0.816794 9:-1.0 10:-1.0 12:-1.0 13:0.5 \n" +
            "-1.0 0.0 0.0 1:-0.333333 2:1.0 3:1.0 4:-0.603774 5:-0.388128 6:-1.0 7:1.0 8:0.740458 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 1.0 0.7758183166944211 1:0.375 2:1.0 3:0.333333 4:-0.320755 5:-0.520548 6:-1.0 7:-1.0 8:0.145038 9:-1.0 10:-0.419355 12:1.0 13:1.0 \n" +
            "-0.47782983903499177 0.0 0.0 1:-0.25 2:1.0 3:0.333333 4:-0.169811 5:-0.401826 6:-1.0 7:1.0 8:0.29771 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-0.09906103805176014 0.0 0.0 1:-0.0833333 2:-1.0 3:0.333333 4:-0.132075 5:-0.16895 6:-1.0 7:1.0 8:0.0839695 9:-1.0 10:-0.516129 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "-0.0 0.08145760604300707 1.0 1:-0.0833333 2:1.0 3:0.333333 4:-0.698113 5:-0.776256 6:-1.0 7:-1.0 8:-0.206107 9:-1.0 10:-0.806452 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 0.3776146634954572 0.32806948071750974 1:0.333333 2:1.0 3:0.333333 4:0.0566038 5:-0.465753 6:1.0 7:-1.0 8:0.00763359 9:1.0 10:-0.677419 12:-1.0 13:-1.0 \n" +
            "-1.0 0.0 0.0 1:-0.0416667 2:1.0 3:0.333333 4:0.471698 5:-0.666667 6:1.0 7:-1.0 8:0.389313 9:-1.0 10:-0.83871 11:-1.0 12:-1.0 13:1.0 \n" +
            "-1.0 0.0 0.0 1:-0.375 2:1.0 3:-0.333333 4:-0.509434 5:-0.374429 6:-1.0 7:-1.0 8:0.557252 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:1.0 \n" +
            "-0.0 0.0 0.11741718309891497 1:0.125 2:-1.0 3:-0.333333 4:-0.132075 5:-0.232877 6:-1.0 7:1.0 8:0.251908 9:-1.0 10:-0.580645 12:-1.0 13:-1.0 \n" +
            "-1.0 1.0 1.0 1:0.166667 2:1.0 3:1.0 4:-0.132075 5:-0.69863 6:-1.0 7:-1.0 8:0.175573 9:-1.0 10:-0.870968 12:-1.0 13:0.5 \n" +
            "-1.0 -1.0 1.0 1:0.166667 2:1.0 3:-0.333333 4:-0.433962 5:-0.383562 6:-1.0 7:-1.0 8:0.0687023 9:-1.0 10:-0.903226 11:-1.0 12:-1.0 13:1.0 \n" +
            "-1.0 -1.0 0.563809071771546 1:2.0 2:2.0 3:2.0 4:2.0 5:2.0 6:2.0 7:2.0 8:1.0839695 9:1.0 10:2.0 12:-0.333333 13:2.0 \n" +
            "-1.0 -1.0 1.0 1:0.25 2:1.0 3:1.0 4:-0.698113 5:-0.484018 6:-1.0 7:1.0 8:0.0839695 9:1.0 10:-0.612903 12:-0.333333 13:1.0 \n" +
            "-1.0 -1.0 1.0 1:0.333333 2:1.0 3:-1.0 4:-0.245283 5:-0.506849 6:-1.0 7:-1.0 8:0.129771 9:-1.0 10:-0.16129 12:0.333333 13:-1.0 \n" +
            "-1.0 -1.0 1.0 1:-0.125 2:1.0 3:1.0 4:-0.0566038 5:-0.6621 6:-1.0 7:1.0 8:-0.160305 9:1.0 10:-0.709677 12:-1.0 13:1.0 \n" +
            "-1.0 -1.0 1.0 1:-0.166667 2:1.0 3:0.333333 4:-0.54717 5:-0.894977 6:-1.0 7:1.0 8:-0.160305 9:-1.0 10:-0.741935 11:-1.0 12:1.0 13:-1.0 \n" +
            "-1.0 -1.0 1.0 1:-0.458333 2:1.0 3:1.0 4:-0.207547 5:-0.136986 6:-1.0 7:-1.0 8:-0.175573 9:1.0 10:-0.419355 12:-1.0 13:0.5 \n" +
            "-1.0 -1.0 1.0 1:0.25 2:-1.0 3:1.0 4:0.509434 5:-0.438356 6:-1.0 7:-1.0 8:0.0992366 9:1.0 10:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 1.0 1:0.333333 2:1.0 3:1.0 4:-0.509434 5:-0.388128 6:-1.0 7:-1.0 8:0.0534351 9:1.0 10:0.16129 12:-0.333333 13:1.0 \n" +
            "-1.0 -1.0 1.0 1:0.583333 2:1.0 3:1.0 4:-0.509434 5:-0.493151 6:-1.0 7:-1.0 8:-1.0 9:-1.0 10:-0.677419 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 1.0 1:-0.166667 2:1.0 3:0.333333 4:-0.509434 5:-0.716895 6:-1.0 7:-1.0 8:0.0381679 9:-1.0 10:-0.354839 12:1.0 13:1.0 \n" +
            "-1.0 -1.0 1.0 1:0.416667 2:-1.0 3:1.0 4:-0.735849 5:-0.347032 6:-1.0 7:-1.0 8:0.496183 9:1.0 10:-0.419355 12:0.333333 13:-1.0 \n" +
            "-1.0 -1.0 1.0 1:0.5 2:1.0 3:-1.0 4:-0.169811 5:-0.287671 6:1.0 7:1.0 8:0.572519 9:-1.0 10:-0.548387 12:-0.333333 13:-1.0 \n" +
            "-1.0 -1.0 0.22349356240260043 1:0.125 2:1.0 3:1.0 4:-0.283019 5:-0.73516 6:-1.0 7:1.0 8:-0.480916 9:1.0 10:-0.322581 12:-0.333333 13:0.5 \n" +
            "-1.0 -1.0 1.0 1:0.291667 2:1.0 3:1.0 4:-0.320755 5:-0.420091 6:-1.0 7:-1.0 8:0.114504 9:1.0 10:-0.548387 11:-1.0 12:-0.333333 13:1.0 \n" +
            "-1.0 -1.0 1.0 1:0.208333 2:1.0 3:-0.333333 4:-0.509434 5:-0.278539 6:-1.0 7:1.0 8:0.358779 9:-1.0 10:-0.419355 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -1.0 1:0.458333 2:1.0 3:1.0 4:-0.358491 5:-0.374429 6:-1.0 7:-1.0 8:-0.480916 9:1.0 10:-0.935484 12:-0.333333 13:1.0 \n" +
            "-1.0 -1.0 -1.0 1:0.875 2:-1.0 3:-0.333333 4:-0.509434 5:-0.347032 6:-1.0 7:1.0 8:-0.236641 9:1.0 10:-0.935484 11:-1.0 12:-0.333333 13:-1.0 \n" +
            "-1.0 -1.0 -1.0 1:0.5 2:1.0 3:1.0 4:-0.509434 5:-0.767123 6:-1.0 7:-1.0 8:0.0534351 9:-1.0 10:-0.870968 11:-1.0 12:-1.0 13:1.0 \n" +
            "-1.0 -1.0 -1.0 1:0.25 2:1.0 3:1.0 4:-0.226415 5:-0.506849 6:-1.0 7:-1.0 8:0.374046 9:-1.0 10:-0.83871 12:-1.0 13:1.0 \n" +
            "-1.0 -1.0 -1.0 1:0.75 2:-1.0 3:1.0 4:-0.660377 5:-0.894977 6:-1.0 7:-1.0 8:-0.175573 9:-1.0 10:-0.483871 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -1.0 1:0.458333 2:1.0 3:-1.0 4:-0.698113 5:-0.611872 6:-1.0 7:1.0 8:0.114504 9:1.0 10:-0.419355 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -0.0 1:-0.25 2:1.0 3:1.0 4:-0.660377 5:-0.643836 6:-1.0 7:-1.0 8:0.0992366 9:-1.0 10:-0.967742 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -1.0 1:-0.291667 2:-1.0 3:1.0 4:-0.169811 5:-0.465753 6:-1.0 7:1.0 8:0.236641 9:1.0 10:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -0.0 1:-0.666667 2:-1.0 3:0.333333 4:-0.509434 5:-0.593607 6:-1.0 7:-1.0 8:0.51145 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -0.7873026341741466 1:-0.458333 2:1.0 3:0.333333 4:-0.320755 5:-0.753425 6:-1.0 7:-1.0 8:0.206107 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -1.0 1:-0.375 2:1.0 3:0.333333 4:-0.320755 5:-0.511416 6:-1.0 7:-1.0 8:0.648855 9:1.0 10:-0.870968 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -1.0 1:-0.333333 2:-1.0 3:-0.333333 4:-0.320755 5:-0.506849 6:-1.0 7:1.0 8:0.587786 9:-1.0 10:-0.806452 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -1.0 1:0.166667 2:1.0 3:1.0 4:-0.698113 5:-0.657534 6:-1.0 7:-1.0 8:-0.160305 9:1.0 10:-0.516129 12:-1.0 13:0.5 \n" +
            "-1.0 -1.0 -1.0 1:0.25 2:1.0 3:1.0 4:-0.169811 5:-0.3379 6:-1.0 7:1.0 8:0.694656 9:-1.0 10:-1.0 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -0.0 1:-0.0833333 2:-1.0 3:0.333333 4:-0.320755 5:-0.406393 6:-1.0 7:1.0 8:0.19084 9:-1.0 10:-0.83871 11:-1.0 12:-1.0 13:-1.0 \n" +
            "-1.0 -1.0 -1.0 1:0.625 2:1.0 3:0.333333 4:-0.54717 5:-0.310502 6:-1.0 7:-1.0 8:0.221374 9:-1.0 10:-0.677419 11:-1.0 12:-0.333333 13:1.0 \n" +
            "-1.0 -1.0 -1.0 1:-0.0833333 2:1.0 3:0.333333 4:-1.0 5:-0.538813 6:-1.0 7:-1.0 8:0.267176 9:1.0 10:-1.0 11:-1.0 12:-0.333333 13:1.0 \n" +
            "-1.0 -1.0 -1.0 1:0.25 2:1.0 3:0.333333 4:0.0566038 5:-0.607306 6:1.0 7:-1.0 8:0.312977 9:-1.0 10:-0.483871 11:-1.0 12:-1.0 13:-1.0 ";

    @Test
    public void multiClassificationNodeTest() {
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("inputKeys", "test1,test2,test3,test4,test5,test6,test7,test8,test9,test10,test11,test12,test13");
        paramMap.put("resultKey", "result");
        paramMap.put("svmType", 0);
        paramMap.put("kernelType", 2);
        paramMap.put("gamma", 0.07692307692307693);
        paramMap.put("labels", "1 -1 2 -2");
        paramMap.put("nSV", "68 75 16 18");
        paramMap.put("rho", "0.3689782313088655 -0.747087406692054 -0.9472660251612454 -0.6753138863524673 -1.0218083384943903 -0.3257255417457472");
        paramMap.put("sVectors", sv);
        Gson gson = new Gson();
        String paramStr = gson.toJson(paramMap);
        TaskNodeParam param = TaskNodeParam.create(paramStr);

        MultiClassificationNode svmNode = new MultiClassificationNode();
        svmNode.setParam(param);
        DataSet in = DataSet.create();
        DataSet.Record record = DataSet.Record.create("{" +
                "\"test13\":1.0," +
                "\"test12\":1.0," +
                "\"test10\":0.290323," +
                "\"label\":1.0," +
                "\"test1\":0.416667," +
                "\"test4\":0.0566038," +
                "\"test5\":0.283105," +
                "\"test2\":-1.0," +
                "\"test3\":1.0," +
                "\"test8\":0.267176," +
                "\"test9\":-1.0," +
                "\"test6\":-1.0," +
                "\"test7\":1.0," +
                "\"id\":10" +
                "}");
        in.addRecord(record);
        String[] key = "test1,test2,test3,test4,test5,test6,test7,test8,test9,test10,test11,test12,test13".split(",");
        List<String> inputkeys = Arrays.asList(key);
        String[] outkey = "result".split(",");
        List<String> outkeys = Arrays.asList(outkey);
        svmNode.calculate(in, inputkeys, outkeys);
        List<String> value = in.getValue("/records/result", List.class);
        Assert.assertEquals(1.0, value.get(0));
    }

    @Test
    public void regresssionNodeTest() {
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("inputKeys", "test1,test2,test3,test4,test5,test6,test7,test8,test9,test10,test11,test12,test13");
        paramMap.put("resultKey", "result");
        paramMap.put("svmType", 3);
        paramMap.put("kernelType", 2);
        paramMap.put("gamma", 0.07692307692307693);
        paramMap.put("rho", 0.225124523813918);
        paramMap.put("sVectors", sv);
        Gson gson = new Gson();
        String paramStr = gson.toJson(paramMap);
        TaskNodeParam param = TaskNodeParam.create(paramStr);

        RegressionNode regressionNode = new RegressionNode();
        regressionNode.setParam(param);
        DataSet in = DataSet.create();
        DataSet.Record record = DataSet.Record.create("{" +
                "\"test1\":0.708333," +
                "\"test2\":1.0," +
                "\"test3\":1.0," +
                "\"test4\":-0.320755," +
                "\"test5\":-0.105023," +
                "\"test6\":-1.0," +
                "\"test7\":1.0," +
                "\"test8\":-0.419847," +
                "\"test9\":-1.0," +
                "\"test10\":-0.225806," +
                "\"test12\":1.0," +
                "\"test13\":-1.0}");
        in.addRecord(record);
        regressionNode.calculate(in, new ArrayList<>(), new ArrayList<>());
        List<Double> values = in.getValue("/records/result", List.class);
        double value = values.get(0);
        Assert.assertEquals(0.4033154169583273, value, 0.1);
    }
}


