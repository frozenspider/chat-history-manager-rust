use tokio::runtime::Handle;
use tonic::transport::Channel;

use crate::choose_myself_service_client::ChooseMyselfServiceClient;

use crate::prelude::*;

pub struct MyselfChooserImpl {
    pub runtime_handle: Handle,
    pub channel: Channel,
}

impl MyselfChooser for MyselfChooserImpl {
    fn choose_myself(&self, users: &[User]) -> Result<usize> {
        let users = users.to_vec();
        let handle = self.runtime_handle.clone();
        let channel = self.channel.clone();

        // We cannot use the current thread since when called via RPC, current thread is already used for async tasks.
        // We're unwrapping join() to propagate panic.
        std::thread::spawn(move || {
            let len = users.len();
            let choose_myself_future = async move {
                let mut client = ChooseMyselfServiceClient::new(channel);
                log::info!("Sending ChooseMyselfRequest");
                client.choose_myself(ChooseMyselfRequest { users })
                    .await.map_err(|status| anyhow!("{}", status.message()))
            };

            let spawned = handle.spawn(choose_myself_future);
            let response = handle.block_on(spawned)?;
            log::info!("Got response: {:?}", response);

            let response = response?.get_ref().picked_option;
            if response < 0 {
                err!("Choice aborted!")
            } else if response as usize >= len {
                err!("Choice out of range!")
            } else {
                Ok(response as usize)
            }
        }).join().unwrap()
    }
}
