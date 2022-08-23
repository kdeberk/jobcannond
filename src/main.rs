mod jobs;
mod protocol;
mod server;
mod session;

use server::run_tcp_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
 let addr = "127.0.0.1:11300".parse::<std::net::SocketAddr>()?;

 let listener = tokio::net::TcpListener::bind(&addr).await?;
 match run_tcp_server(listener).await {
  Ok(_) => (),
  Err(e) => println!("{:?}", e),
 };
 Ok(())
}
