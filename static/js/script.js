function draw(){
        var config = {
            container_id: "viz",
            server_url: "bolt://localhost:7687",
            server_user: "neo4j",
            server_password: "test",
            labels:{
                 "address_details":{
                    "caption": "name",
                    "size": 200,
					"community": "community",
					"title_properties":[
						"Rank",
						"addr_id",
						"addr_line_1",
						"addr_line_2",
						"city",
						"state",
						"zip"
					]

                },
				 "zipcodes":{
                    "caption": "name",
                    "size": 50,
					"title_properties":[
						"zip_no"
					]
               },
				"citys":{
                    "caption": "name",
                    "size": 50,
					"title_properties":[
						"city_name"
					]
               },
				"states":{
                    "caption": "name",
                    "size": 75,
					"title_properties":[
						"state_name"
					]
               },
				"prescribers":{
                    "caption": "name",
                    "size": 150,
					"title_properties":[
						"presc_id"
					]
               }
            },
            relationships:{
               "prescribe":{
                    thickness: "weight",
                    caption : false,
                    community: 11,
                }
            },
            initial_cypher: "MATCH p=(n:address_details {state: 'NJ'})<- [] - () return p"
        };
        var viz = new NeoVis.default(config);
        viz.render();

}
