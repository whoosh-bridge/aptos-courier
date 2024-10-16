
use std::fmt::Debug;
use std::io::Cursor;
use prost::Message;
use tonic::transport::{Channel, Error};
use tonic::{Response, Status, Streaming};
use tokio;

/// GRPC request metadata key for the token ID.
const GRPC_API_GATEWAY_API_KEY_HEADER: &str = "authorization";
/// GRPC request metadata key for the request name. This is used to identify the
/// data destination.
const GRPC_REQUEST_NAME_HEADER: &str = "x-aptos-request-name";
/// GRPC connection id
const GRPC_CONNECTION_ID: &str = "x-aptos-connection-id";
/// We will try to reconnect to GRPC 5 times in case upstream connection is being updated
pub const RECONNECTION_MAX_RETRIES: u64 = 5;
/// 256MB
pub const MAX_RESPONSE_SIZE: usize = 1024 * 1024 * 256;

// Include the `items` module, which is generated from items.proto.
pub mod aptos {
    pub mod transaction{
        pub mod v1{
            include!(concat!(env!("OUT_DIR"), "/aptos.transaction.v1.rs"));
        }
    }    
    pub mod util{
        pub mod timestamp{
            include!(concat!(env!("OUT_DIR"), "/aptos.util.timestamp.rs"));
        }
    }

    pub mod indexer{
        pub mod v1{
            include!(concat!(env!("OUT_DIR"), "/aptos.indexer.v1.rs"));
        }
    }
}

pub fn grpc_request_builder(
    starting_version: u64,
    transactions_count: Option<u64>,
    grpc_auth_token: String,
    processor_name: String,
) -> tonic::Request<aptos::indexer::v1::GetTransactionsRequest> {
    let mut request = tonic::Request::new(aptos::indexer::v1::GetTransactionsRequest {
        starting_version: Some(starting_version),
        transactions_count,
        ..aptos::indexer::v1::GetTransactionsRequest::default()
    });
    request.metadata_mut().insert(
        GRPC_API_GATEWAY_API_KEY_HEADER,
        format!("Bearer {}", grpc_auth_token.clone())
            .parse()
            .unwrap(),
    );
    request
        .metadata_mut()
        .insert(GRPC_REQUEST_NAME_HEADER, processor_name.parse().unwrap());
    request
}


async fn streaming_transactions(){   
    use aptos::indexer::v1::raw_data_client::RawDataClient;
    use aptos::indexer::v1::TransactionsResponse;

    let mut connection_result = RawDataClient::connect("http://127.0.0.1:50051/").await;


    let mut rpc_client: RawDataClient<Channel>;

    match connection_result {
        Ok(client) =>{
            rpc_client = client;
            println!("connected!");
        },
        Err(_) =>{
            println!("Failed to connect to local node! Make sure you're running the node.");
            return;
        },        
    }    

    rpc_client = rpc_client
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd)
        .max_decoding_message_size(MAX_RESPONSE_SIZE)
        .max_encoding_message_size(MAX_RESPONSE_SIZE);      

    let mut starting_version: u64 = 0; 
    loop{
        let request = grpc_request_builder(starting_version,Some(10),"".into(),"".into());
        let request_result: Result<Response<Streaming<TransactionsResponse>>, Status> = rpc_client.get_transactions(request).await;
        let stream: Response<Streaming<TransactionsResponse>>;
        match request_result {
            Ok(s)=>{
                stream = s;            
            },
            Err(_)=>{
                println!("Request failed!");
                return;
            }
        }
        
        let response_result = stream.into_inner().message().await;
        let response: Option<TransactionsResponse>;
        
        match response_result{
            Ok(res) =>{
                response = res;
            },
            Err(_) =>{
                println!("Failed to read response!");
                return;
            }     
        }

        match response {            
            Some(txns) if txns.transactions.len() ==0 =>{
                println!("No new transction!");
            }
            Some(txns) =>{
                println!("starting version: {starting_version}");

                match txns.transactions.last() {
                    None=>{
                        starting_version = starting_version + 10;
                    },
                    Some(last_txn) => {
                        starting_version = last_txn.version + 10;
                    }                    
                }
                for txn in txns.transactions{                                                                               
                    println!("transaction version: {}",txn.version);
                    println!("transaction blockheight: {}",txn.block_height);
                    match txn.txn_data{
                        Some(data)=>{
                            use aptos::transaction::v1::transaction::TxnData;
                            match data {                            
                                TxnData::User(tx) =>{
                                    println!("{}","user transaction received!");
                                    println!("{}",tx.request.unwrap().sender);                                
                                },
                                TxnData::Genesis(tx) =>{
                                    println!("{}","genesis transaction received!");
                                },                                                           
                                TxnData::BlockMetadata(metadata) =>{
                                    println!("{}","meta data transaction received!");
                                },
                                TxnData::Validator(v) =>{
                                    println!("validator transaction received.");
                                },
                                TxnData::BlockEpilogue(e)=>{
                                    println!("Epilogue transaction received.");
                                },
                                TxnData::StateCheckpoint(checkpoint) =>{
                                    println!("Checkpoint received.");
                                }

                            }
                        },
                        None =>{
                            println!("transaction has no data!");
                        }                        
                        
                    }                
                    
                }
                
            },
            None=>{
                println!("End of transactions");
            }
        }

        
    }    
    // let mut stream = stream.take(1);
    
    // while let Some(txn) = stream.next().await {
    //     println!("\treceived: {}", txn.unwrap());
    // }
}


fn main() {    
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();    
    let res = rt.block_on(async { streaming_transactions().await; });        
}
